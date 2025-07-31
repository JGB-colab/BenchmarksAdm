import psycopg2
import random
import threading
import time
import queue
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_READ_COMMITTED

# ==============================================================================
# --- CONFIGURAÇÕES DO TESTE E DO BANCO DE DADOS ---
# ==============================================================================

# Parâmetros do Teste
NUM_WORKERS = 4                 # Número de threads/usuários simultâneos
TEST_DURATION_SECONDS = 120    # Duração do teste em segundos

# Nomes dos Bancos de Dados
DB_TEMPLATE_NAME = "tpce_clean"  # O nome do seu banco de dados modelo
DB_TEST_NAME = "postgres"       # O nome do banco de dados temporário que será criado e destruído

# Configuração de Conexão para o Teste (os workers usam esta)
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "user": "postgres",
    "password": "123",
    "dbname": DB_TEST_NAME
}

# Configuração de Conexão para tarefas de Administração (reset do banco)
DB_ADMIN_CONFIG = DB_CONFIG.copy()
DB_ADMIN_CONFIG["dbname"] = "postgres" # Conecta-se a um banco padrão para poder criar/dropar outros

# Filas para comunicação entre threads
task_queue = queue.Queue()
results_queue = queue.Queue()

def reset_test_database():
    print("="*60)
    print(f"--- RESETANDO O AMBIENTE DE TESTE ---")
    conn = None
    try:
        conn = psycopg2.connect(**DB_ADMIN_CONFIG, connect_timeout=5)
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        print(f"1. Derrubando banco de dados '{DB_TEST_NAME}' (se existir)...")
        cursor.execute(f"DROP DATABASE IF EXISTS {DB_TEST_NAME};")
        print(f"2. Criando banco de dados '{DB_TEST_NAME}' a partir do template '{DB_TEMPLATE_NAME}'...")
        cursor.execute(f"CREATE DATABASE {DB_TEST_NAME} WITH TEMPLATE {DB_TEMPLATE_NAME};")
        print("--- AMBIENTE DE TESTE PRONTO ---")
        cursor.close()
    except psycopg2.Error as e:
        print(f"\nERRO FATAL ao resetar banco: {e}")
        exit(1)
    finally:
        if conn: conn.close()
        print("="*60)


# ==============================================================================
# --- DEFINIÇÃO DAS TRANSAÇÕES TPC-E  ---
# ==============================================================================
def run_broker_volume(cursor):
    """ Tabelas usadas: e_trade_request, e_sector, e_industry, e_company, e_broker, e_security """
    sector_name = random.choice(["Technology", "Financial", "Energy", "Healthcare"])
    broker_list = [f"Broker-{random.randint(1, 100)}" for _ in range(random.randint(2, 5))]
    query = """
        SELECT b_name, SUM(tr_qty * tr_bid_price) AS price_sum
        FROM e_trade_request, e_sector, e_industry, e_company, e_broker, e_security
        WHERE tr_b_id = b_id AND tr_s_symb = s_symb AND s_co_id = co_id
          AND co_in_id = in_id AND sc_id = in_sc_id
          AND b_name IN %s AND sc_name = %s
        GROUP BY b_name ORDER BY price_sum DESC
    """
    cursor.execute(query, (tuple(broker_list), sector_name))
    cursor.fetchall()

def run_customer_position(cursor):
    """ Tabelas usadas: e_customer, e_trade_bak, e_trade_history, e_status_type """
    cust_id = random.randint(1, 500000)
    cursor.execute("SELECT c_id FROM e_customer WHERE c_id = %s", (cust_id,))
    cursor.fetchone()
    cursor.execute("""
        SELECT t_id, t_s_symb, t_qty, st_name
        FROM (SELECT t_id AS id FROM e_trade_bak WHERE t_ca_id = %s ORDER BY t_dts DESC LIMIT 10) AS t,
             e_trade_bak, e_trade_history_bak, e_status_type
        WHERE t_id = id AND th_t_id = t_id AND st_id = th_st_id
        ORDER BY th_dts DESC LIMIT 30
    """, (cust_id,))
    cursor.fetchall()

def run_market_feed(cursor):
    """ Tabela usada: e_last_trade """
    symbol = f"SYM{random.randint(1, 1000)}"
    price = round(random.uniform(10.0, 500.0), 2)
    trade_qty = random.randint(10, 1000)
    now_dts = time.strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("UPDATE e_last_trade SET lt_price = %s, lt_vol = lt_vol + %s, lt_dts = %s WHERE lt_s_symb = %s", (price, trade_qty, now_dts, symbol))

def run_market_watch(cursor):
    """ Tabelas usadas: e_watch_item, e_watch_list, e_last_trade, e_security, e_daily_market """
    cust_id = random.randint(1, 500000)
    start_date = "2023-01-01" 
    cursor.execute("""
        SELECT COALESCE(SUM(s_num_out * dm_close), 0), COALESCE(SUM(s_num_out * lt_price), 0)
        FROM e_watch_item, e_watch_list, e_last_trade, e_security, e_daily_market
        WHERE wl_c_id = %s AND wi_wl_id = wl_id AND dm_s_symb = wi_s_symb
          AND dm_date = %s AND lt_s_symb = dm_s_symb AND s_symb = dm_s_symb
    """, (cust_id, start_date))
    cursor.fetchone()

def run_security_detail(cursor):
    """ Tabelas usadas: e_security, e_company, e_exchange """
    symbol = f"SYM{random.randint(1, 1000)}"
    cursor.execute("SELECT s_name, co_name, ex_name FROM e_security, e_company, e_exchange WHERE s_symb = %s AND s_co_id = co_id AND s_ex_id = ex_id", (symbol,))
    cursor.fetchone()

def run_trade_lookup(cursor):
    """ Tabela usada: e_trade """
    acct_id = random.randint(1, 500000)
    start_dts, end_dts = "2023-01-01 00:00:00", "2023-12-31 23:59:59"
    cursor.execute("SELECT t_id, t_exec_name FROM e_trade_bak WHERE t_ca_id = %s AND t_dts >= %s AND t_dts <= %s ORDER BY t_dts LIMIT 20", (acct_id, start_dts, end_dts))
    cursor.fetchall()

def run_trade_order(cursor):
    """ Tabelas usadas: e_customer_account, e_trade (para INSERT) """
    acct_id = random.randint(1, 500000)
    symbol = f"SYM{random.randint(1, 1000)}"
    trade_qty = random.randint(10, 500)
    cursor.execute("SELECT ca_name FROM e_customer_account WHERE ca_id = %s", (acct_id,))
    if cursor.fetchone():
        now_dts = time.strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("SELECT nextval('public.e_seq_trade_id')")
        trade_id = cursor.fetchone()[0]
        cursor.execute("""
            INSERT INTO e_trade_bak(t_id, t_dts, t_st_id, t_tt_id, t_is_cash, t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name, t_chrg, t_comm, t_lifo)
            VALUES (%s, %s, 'P', 'LMT', true, %s, %s, %s, %s, 'Xavier', 2.99, 1.99, true)
        """, (trade_id, now_dts, symbol, trade_qty, round(random.uniform(10,500),2), acct_id))

def run_trade_result(cursor):
    """ Tabelas usadas: e_customer_account, e_trade_bak, e_trade_history_bak """
    acct_id = random.randint(1, 500000)
    symbol = f"SYM{random.randint(1, 1000)}"
    trade_qty = random.randint(10, 500)
    cursor.execute("SELECT ca_id FROM e_customer_account WHERE ca_id = %s", (acct_id,))
    if cursor.fetchone():
        now_dts = time.strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute("SELECT nextval('public.e_seq_trade_id')")
        trade_id = cursor.fetchone()[0]
        cursor.execute("""
            INSERT INTO e_trade_bak(t_id, t_dts, t_st_id, t_tt_id, t_is_cash, t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name, t_chrg, t_comm, t_lifo)
            VALUES (%s, %s, 'P', 'LMT', true, %s, %s, %s, %s, 'AutoResult', 2.99, 1.99, true)
        """, (trade_id, now_dts, symbol, trade_qty, round(random.uniform(10,500),2), acct_id))
        cursor.execute("UPDATE e_trade_bak SET t_st_id = 'C' WHERE t_id = %s", (trade_id,))
        cursor.execute("INSERT INTO e_trade_history_bak(th_t_id, th_dts, th_st_id) VALUES (%s, %s, 'C')", (trade_id, now_dts))

def run_trade_status(cursor):
    """ Tabelas usadas: e_trade_bak, e_status_type, e_trade_type """
    acct_id = random.randint(1, 500000)
    cursor.execute("""
        SELECT t_id, st_name, tt_name, t_s_symb, t_qty, t_exec_name
        FROM e_trade_bak, e_status_type, e_trade_type
        WHERE t_ca_id = %s AND t_st_id = st_id AND t_tt_id = tt_id
        ORDER BY t_dts DESC LIMIT 50
    """, (acct_id,))
    cursor.fetchall()

def run_trade_update(cursor):
    """ Tabela usada: e_trade_bak(para SELECT e UPDATE) """
    cursor.execute("SELECT t_id FROM e_trade_bak ORDER BY t_dts DESC LIMIT 20")
    trade_ids = [row[0] for row in cursor.fetchall()]
    if trade_ids:
        trade_to_update = random.choice(trade_ids)
        new_exec_name = f"Exec-{random.randint(1000, 9999)}"
        cursor.execute("UPDATE e_trade_bak SET t_exec_name = %s WHERE t_id = %s", (new_exec_name, trade_to_update))

AVAILABLE_TRANSACTIONS = [run_broker_volume, run_customer_position, run_market_feed, run_market_watch, run_security_detail, run_trade_lookup, run_trade_order, run_trade_result, run_trade_status, run_trade_update]

# ==============================================================================
# --- CLASSE DO WORKER (THREAD) ---
# ==============================================================================
class WorkerThread(threading.Thread):
    def __init__(self, name, task_q, result_q, test_start_time):
        super().__init__()
        self.name = name
        self.task_q = task_q
        self.result_q = result_q
        self.test_start_time = test_start_time
        self.stop_event = threading.Event()
        self.conn = None
        self.cursor = None

    def run(self):
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.conn.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)
            self.cursor = self.conn.cursor()
        except psycopg2.Error as e:
            print(f"{self.name}: ERRO DE CONEXÃO - {e}")
            self.result_q.put(('CONNECTION_ERROR', 0, 'FATAL', 0))
            return

        while not self.stop_event.is_set():
            try:
                transaction_func = self.task_q.get(timeout=0.1)
                start_time = time.monotonic()
                status = "SUCCESS"
                
                try:
                    transaction_func(self.cursor)
                    self.conn.commit()
                except psycopg2.Error as e:
                    self.conn.rollback()
                    if hasattr(e, 'pgcode') and e.pgcode == '40001':
                        status = "DEADLOCK"
                    else:
                        status = "ABORT"
                
                end_time = time.monotonic()
                duration = end_time - start_time
                completion_timestamp = end_time - self.test_start_time
                self.result_q.put((transaction_func.__name__, duration, status, completion_timestamp))
                self.task_q.task_done()
            except queue.Empty:
                continue
        
        if self.conn:
            self.cursor.close()
            self.conn.close()

    def stop(self):
        self.stop_event.set()

# ==============================================================================
# --- FUNÇÃO DE GERAÇÃO DE RELATÓRIO E GRÁFICOS ---
# ==============================================================================
def generate_report_and_graphs(results_df, duration):
    """Calcula métricas, imprime um relatório e gera gráficos de desempenho."""
    if results_df.empty:
        print("Nenhum resultado foi coletado para gerar o relatório.")
        return

    # --- Cálculos ---
    total_transactions = len(results_df)
    throughput = total_transactions / duration if duration > 0 else 0
    success_count = len(results_df[results_df['status'] == 'SUCCESS'])
    abort_count = len(results_df[results_df['status'] == 'ABORT'])
    deadlock_count = len(results_df[results_df['status'] == 'DEADLOCK']) # BUG CORRIGIDO: Usava 'df' em vez de 'results_df'
    avg_response_time_ms = (results_df['duration'].sum() / total_transactions * 1000) if total_transactions > 0 else 0
    error_rate_percent = ((abort_count + deadlock_count) / total_transactions * 100) if total_transactions > 0 else 0

    # --- Relatório no Console ---
    print("\n\n" + "="*24 + " RELATÓRIO FINAL (PostgreSQL) " + "="*24)
    print(f"\n[Métricas Gerais]")
    print(f"  - Duração do Teste:       {duration:.2f} segundos")
    print(f"  - Total de Transações:    {total_transactions}")
    print(f"  - Vazão (Throughput):     {throughput:.2f} transações/segundo")
    print(f"  - Tempo de Resposta Médio:{avg_response_time_ms:.2f} ms")
    
    print(f"\n[Detalhes de Execução]")
    print(f"  - Sucessos:               {success_count}")
    print(f"  - Aborts (Erros Gerais):  {abort_count}")
    print(f"  - Deadlocks:              {deadlock_count}")
    print(f"  - Taxa Total de Erros:    {error_rate_percent:.2f}%")
    
    print(f"\n[Estatísticas por Transação]")
    grouped_stats = results_df.groupby('name').agg(
        total_execucoes=('name', 'count'),
        tempo_medio_ms=('duration', lambda x: x.mean() * 1000),
        sucessos=('status', lambda x: (x == 'SUCCESS').sum()),
        aborts=('status', lambda x: (x == 'ABORT').sum()),
        deadlocks=('status', lambda x: (x == 'DEADLOCK').sum())
    )
    with pd.option_context('display.max_rows', None,
                           'display.max_columns', None,
                           'display.width', 1000,
                           'display.colheader_justify', 'center'):
        print(grouped_stats.to_string(float_format="%.2f"))

    print("\n" + "="*70)

    # --- Geração de Gráficos ---
    print("\nGerando gráficos...")
    sns.set_theme(style="whitegrid")

    # Gráfico de Vazão
    fig1, ax1 = plt.subplots(figsize=(12, 6))
    results_df['second'] = results_df['timestamp'].astype(int)
    throughput_data = results_df.groupby('second').size()
    sns.lineplot(x=throughput_data.index, y=throughput_data.values, ax=ax1, color='royalblue', lw=2)
    ax1.set_title('Vazão Durante o Teste (PostgreSQL)', fontsize=16)
    ax1.set_xlabel('Tempo (segundos)', fontsize=12)
    ax1.set_ylabel('Vazão (TPS)', fontsize=12)
    fig1.tight_layout()
    fig1.savefig('grafico_vazao_postgres.png', dpi=150)
    plt.close(fig1)

    # Gráfico de Tempo de Resposta
    fig2, ax2 = plt.subplots(figsize=(10, 8))
    avg_response_times = results_df.groupby('name')['duration'].mean() * 1000
    avg_response_times = avg_response_times.sort_values(ascending=False)
    sns.barplot(x=avg_response_times.values, y=avg_response_times.index, ax=ax2, palette='viridis')
    ax2.set_title('Tempo de Resposta Médio por Transação (PostgreSQL)', fontsize=16)
    ax2.set_xlabel('Tempo de Resposta (ms)', fontsize=12)
    ax2.set_ylabel('Transação', fontsize=12)
    ax2.bar_label(ax2.containers[0], fmt='%.0f ms', padding=3)
    fig2.tight_layout()
    fig2.savefig('grafico_tempo_resposta_postgres.png', dpi=150)
    plt.close(fig2)
    
    print("Gráficos salvos como 'grafico_vazao_postgres.png' e 'grafico_tempo_resposta_postgres.png'")

# ==============================================================================
# --- BLOCO DE EXECUÇÃO PRINCIPAL ---
# ==============================================================================
if __name__ == "__main__":
    # Passo 1: Preparar o ambiente de teste, garantindo um estado limpo.
    #reset_test_database()
    
    # Passo 2: Iniciar o teste de carga.
    print(f"\n--- INICIANDO TESTE DE CARGA ---")
    print(f"Workers: {NUM_WORKERS} | Duração: {TEST_DURATION_SECONDS} segundos\n")
    
    test_start_time = time.monotonic()
    workers = []
    for i in range(NUM_WORKERS):
        worker = WorkerThread(f"Worker-{i+1}", task_queue, results_queue, test_start_time)
        worker.start()
        workers.append(worker)

    # A thread principal atua como um "gerador de carga", enchendo a fila de tarefas.
    while time.monotonic() - test_start_time < TEST_DURATION_SECONDS:
        task_queue.put(random.choice(AVAILABLE_TRANSACTIONS))
        # Um pequeno sleep para não sobrecarregar a CPU da máquina de teste
        # com a geração de tarefas, permitindo que os workers trabalhem.
        time.sleep(0.001)

    print("\n--- Tempo esgotado. Finalizando workers... ---")
    for worker in workers:
        worker.stop()
    for worker in workers:
        worker.join()

    # Passo 3: Coletar os resultados e gerar o relatório.
    print("--- Coletando e processando resultados... ---")
    results = []
    while not results_queue.empty():
        results.append(results_queue.get())

    df = pd.DataFrame(results, columns=['name', 'duration', 'status', 'timestamp'])
    
    # Chama a função de relatório unificada
    generate_report_and_graphs(df, TEST_DURATION_SECONDS)