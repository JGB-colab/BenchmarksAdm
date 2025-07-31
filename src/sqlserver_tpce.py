import threading
import time
import random
import queue
import os
import pyodbc
import pandas as pd
import matplotlib.pyplot as plt

# --- CONFIGURAÇÕES DO TESTE ---
NUM_WORKERS = 4
TEST_DURATION_SECONDS = 120

# --- CONFIGURAÇÕES DO BANCO DE DADOS  ---

def get_connection_string():
    driver = "ODBC Driver 17 for SQL Server"
    server = "localhost,1430"
    db_name = "master"
    user = "sa"
    password = "SenhaForte#123"
    return f"DRIVER={driver};SERVER={server};DATABASE={db_name};UID={user};PWD={password}"


# --- FILAS ---
task_queue = queue.Queue()
results_queue = queue.Queue()

# ==============================================================================
# DEFINIÇÃO DAS TRANSAÇÕES TPC-E 
# ==============================================================================

def run_broker_volume(cursor):
    """ TPC-E Broker-Volume Transaction """
    sector_name = random.choice(["Technology", "Financial", "Energy", "Healthcare"])
    broker_list = [f"Broker-{random.randint(1, 100)}" for _ in range(random.randint(2, 5))]
    
    placeholders = ','.join('?' for _ in broker_list)
    params = broker_list + [sector_name]
    
    query = f"""
        SELECT b_name, SUM(tr_qty * tr_bid_price) AS price_sum
        FROM e_trade_request, e_sector, e_industry, e_company, e_broker, e_security
        WHERE tr_b_id = b_id AND tr_s_symb = s_symb AND s_co_id = co_id
          AND co_in_id = in_id AND sc_id = in_sc_id
          AND b_name IN ({placeholders}) AND sc_name = ?
        GROUP BY b_name ORDER BY price_sum DESC
    """
    cursor.execute(query, params)
    cursor.fetchall()

def run_customer_position(cursor):
    """ TPC-E Customer-Position Transaction """
    cust_id = random.randint(1, 500000)
    cursor.execute("SELECT c_id FROM e_customer WHERE c_id = ?", cust_id)
    cursor.fetchone()
    cursor.execute("""
        SELECT TOP 30 t.t_id, t.t_s_symb, t.t_qty, st.st_name
        FROM (SELECT TOP 10 t_id AS id FROM e_trade_bak WHERE t_ca_id = ? ORDER BY t_dts DESC) AS t_alias
        JOIN e_trade_bak t ON t.t_id = t_alias.id
        JOIN e_trade_history_bak th ON th.th_t_id = t.t_id
        JOIN e_status_type st ON st.st_id = th.th_st_id
        ORDER BY th.th_dts DESC
    """, cust_id)
    cursor.fetchall()

def run_market_feed(cursor):
    """ TPC-E Market-Feed Transaction """
    symbol = f"SYM{random.randint(1, 1000)}"
    price = round(random.uniform(10.0, 500.0), 2)
    trade_qty = random.randint(10, 1000)
    now_dts = time.strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute("""
        UPDATE e_last_trade SET lt_price = ?, lt_vol = lt_vol + ?, lt_dts = ?
        WHERE lt_s_symb = ?
    """, price, trade_qty, now_dts, symbol)
 
def run_market_watch(cursor):
    """ TPC-E Market-Watch Transaction """
    cust_id = random.randint(1, 500000)
    start_date = "2023-01-01" 
    cursor.execute("""
        SELECT COALESCE(SUM(s_num_out * dm_close), 0), COALESCE(SUM(s_num_out * lt_price), 0)
        FROM e_watch_item, e_watch_list, e_last_trade, e_security, e_daily_market
        WHERE wl_c_id = ? AND wi_wl_id = wl_id AND dm_s_symb = wi_s_symb
          AND dm_date = ? AND lt_s_symb = dm_s_symb AND s_symb = dm_s_symb
    """, cust_id, start_date)
    cursor.fetchone()

def run_security_detail(cursor):
    """ TPC-E Security-Detail Transaction """
    symbol = f"SYM{random.randint(1, 1000)}"
    cursor.execute("SELECT s_name, co_name, ex_name FROM e_security, e_company, e_exchange WHERE s_symb = ? AND s_co_id = co_id AND s_ex_id = ex_id", (symbol,))
    cursor.fetchone()

def run_trade_lookup(cursor):
    """ TPC-E Trade-Lookup Transaction """
    acct_id = random.randint(1, 500000)
    start_dts, end_dts = "2023-01-01 00:00:00", "2023-12-31 23:59:59"
    cursor.execute("SELECT TOP 20 t_id, t_exec_name FROM e_trade_bak WHERE t_ca_id = ? AND t_dts >= ? AND t_dts <= ? ORDER BY t_dts", (acct_id, start_dts, end_dts))
    cursor.fetchall()

def run_trade_order(cursor):
    """ TPC-E Trade-Order Transaction """
    acct_id = random.randint(1, 500000)
    symbol = f"SYM{random.randint(1, 1000)}"
    trade_qty = random.randint(10, 500)
    cursor.execute("SELECT ca_name, ca_b_id, ca_c_id FROM e_customer_account WHERE ca_id = ?", acct_id)
    ca_info = cursor.fetchone()
    if ca_info:
        now_dts = time.strftime('%Y-%m-%d %H:%M:%S')
        status_id, trade_type_id, exec_name = 'P', 'LMT', "Xavier"
        is_cash, is_lifo, charge, comm = 1, 1, 2.99, 1.99
        cursor.execute("SELECT NEXT VALUE FOR dbo.e_seq_trade_id")
        trade_id = cursor.fetchone()[0]
        cursor.execute("""
            INSERT INTO e_trade_bak(t_id, t_dts, t_st_id, t_tt_id, t_is_cash, t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name, t_chrg, t_comm, t_lifo)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, trade_id, now_dts, status_id, trade_type_id, is_cash, symbol, trade_qty, round(random.uniform(10,500),2), acct_id, exec_name, charge, comm, is_lifo)

def run_trade_result(cursor):
    """ TPC-E Trade-Result Transaction (autossuficiente) """
    acct_id = random.randint(1, 500000)
    symbol = f"SYM{random.randint(1, 1000)}"
    trade_qty = random.randint(10, 500)
    cursor.execute("SELECT ca_id FROM e_customer_account WHERE ca_id = ?", (acct_id,))
    if cursor.fetchone():
        now_dts = time.strftime('%Y-%m-%d %H:%M:%S')
        status_id, trade_type_id, exec_name = 'P', 'LMT', "AutoResult"
        is_cash, is_lifo, charge, comm = 1, 1, 2.99, 1.99
        cursor.execute("SELECT NEXT VALUE FOR dbo.e_seq_trade_id")
        trade_id = cursor.fetchone()[0]
        cursor.execute("""
            INSERT INTO e_trade_bak(t_id, t_dts, t_st_id, t_tt_id, t_is_cash, t_s_symb, t_qty, t_bid_price, t_ca_id, t_exec_name, t_chrg, t_comm, t_lifo)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, trade_id, now_dts, status_id, trade_type_id, is_cash, symbol, trade_qty, round(random.uniform(10,500),2), acct_id, exec_name, charge, comm, is_lifo)
        cursor.execute("UPDATE e_trade_bak SET t_st_id = 'C' WHERE t_id = ?", trade_id)
        cursor.execute("INSERT INTO e_trade_history_bak(th_t_id, th_dts, th_st_id) VALUES (?, ?, 'C')", (trade_id, now_dts))

def run_trade_status(cursor):
    """ TPC-E Trade-Status Transaction """
    acct_id = random.randint(1, 500000)
    cursor.execute("""
        SELECT TOP 50 t_id, st_name, tt_name, t_s_symb, t_qty, t_exec_name
        FROM e_trade_bak, e_status_type, e_trade_type
        WHERE t_ca_id = ? AND t_st_id = st_id AND t_tt_id = tt_id
        ORDER BY t_dts DESC
    """, (acct_id,))
    cursor.fetchall()

def run_trade_update(cursor):
    """ TPC-E Trade-Update Transaction """
    cursor.execute("SELECT TOP 20 t_id FROM e_trade_bak ORDER BY t_dts DESC")
    trade_ids = [row[0] for row in cursor.fetchall()]
    if trade_ids:
        trade_to_update = random.choice(trade_ids)
        new_exec_name = f"Exec-{random.randint(1000, 9999)}"
        cursor.execute("UPDATE e_trade_bak SET t_exec_name = ? WHERE t_id = ?", (new_exec_name, trade_to_update))

AVAILABLE_TRANSACTIONS = [run_broker_volume, run_customer_position, run_market_feed, run_market_watch, run_security_detail, run_trade_lookup, run_trade_order, run_trade_result, run_trade_status, run_trade_update]

# ==============================================================================
# CLASSE DO WORKER (THREAD) PARA PYODBC
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

    def run(self):
        try:
            self.conn = pyodbc.connect(get_connection_string(), autocommit=False)
        except pyodbc.Error as e:
            print(f"{self.name}: ERRO DE CONEXÃO - {e}")
            self.result_q.put(('CONNECTION_ERROR', 0, 'FATAL', 0))
            return
        
        cursor = self.conn.cursor()

        while not self.stop_event.is_set():
            try:
                transaction_func = self.task_q.get(timeout=0.1)
                start_time = time.monotonic()
                status = "SUCCESS"
                
                try:
                    transaction_func(cursor)
                    self.conn.commit()
                except pyodbc.Error as e:
                    self.conn.rollback()
                    if '40001' in str(e) or '1205' in str(e):
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
            cursor.close()
            self.conn.close()

    def stop(self):
        self.stop_event.set()

# ==============================================================================
# GERAÇÃO DE GRÁFICOS E RELATÓRIO
# ==============================================================================
def generate_graphs(results_df):
    if results_df.empty: return
    print("Gerando gráficos...")
    plt.style.use('seaborn-v0_8-whitegrid')
    
    # Gráfico de Vazão
    fig1, ax1 = plt.subplots(figsize=(12, 6))
    results_df['second'] = results_df['timestamp'].astype(int)
    throughput_data = results_df.groupby('second').size()
    ax1.plot(throughput_data.index, throughput_data.values, color='dodgerblue')
    ax1.set_title('Vazão Durante o Teste (SQL Server)', fontsize=16)
    ax1.set_xlabel('Tempo (segundos)'); ax1.set_ylabel('Transações por Segundo (vazão)')
    fig1.savefig('grafico_vazao_sqlserver.png', dpi=150)
    plt.close(fig1)

    # Gráfico de Tempo de Resposta
    fig2, ax2 = plt.subplots(figsize=(10, 8))
    avg_response_times = results_df.groupby('name')['duration'].mean() * 1000
    avg_response_times = avg_response_times.sort_values()
    bars = ax2.barh(avg_response_times.index, avg_response_times.values, color='dodgerblue')
    ax2.set_title('Tempo de Resposta Médio por Transação (SQL Server)', fontsize=16)
    ax2.set_xlabel('Tempo de Resposta (ms)'); ax2.set_ylabel('Transação')
    for bar in bars:
        width = bar.get_width()
        ax2.text(width + 5, bar.get_y() + bar.get_height()/2, f'{width:.0f} ms', ha='left', va='center')
    plt.tight_layout()
    fig2.savefig('grafico_tempo_resposta_sqlserver.png', dpi=150)
    plt.close(fig2)
    print("Gráficos salvos como 'grafico_vazao_sqlserver.png' e 'grafico_tempo_resposta_sqlserver.png'")

if __name__ == "__main__":
    print(f"--- Iniciando Teste TPC-E para SQL Server (READ COMMITTED): {NUM_WORKERS} Workers, {TEST_DURATION_SECONDS} segundos ---")
    test_start_time = time.monotonic()
    workers = []
    for i in range(NUM_WORKERS):
        worker = WorkerThread(f"Worker-{i+1}", task_queue, results_queue, test_start_time)
        worker.start()
        workers.append(worker)

    while time.monotonic() - test_start_time < TEST_DURATION_SECONDS:
        task_queue.put(random.choice(AVAILABLE_TRANSACTIONS))
        time.sleep(0.001)

    print("\n--- Tempo esgotado. Finalizando workers... ---")
    for worker in workers:
        worker.stop()
    for worker in workers:
        worker.join()

    print("--- Coletando resultados... ---")
    results = []
    while not results_queue.empty():
        results.append(results_queue.get())

    df = pd.DataFrame(results, columns=['name', 'duration', 'status', 'timestamp'])
    
    total_transactions = len(df)
    throughput = total_transactions / TEST_DURATION_SECONDS if TEST_DURATION_SECONDS > 0 else 0
    success_count = len(df[df['status'] == 'SUCCESS'])
    abort_count = len(df[df['status'] == 'ABORT'])
    deadlock_count = len(df[df['status'] == 'DEADLOCK'])
    avg_response_time_ms = (df['duration'].sum() / total_transactions * 1000) if total_transactions > 0 else 0
    abort_rate_percent = ((abort_count + deadlock_count) / total_transactions * 100) if total_transactions > 0 else 0

    print("\n\n========== RELATÓRIO FINAL (SQL Server) ==========")
    print(f"\n[Métricas Gerais]")
    print(f"  - Duração do Teste:       {TEST_DURATION_SECONDS:.2f} segundos")
    print(f"  - Total de Transações:    {total_transactions}")
    print(f"  - Vazão (Throughput):     {throughput:.2f} transações/segundo")
    print(f"  - Tempo de Resposta Médio:{avg_response_time_ms:.2f} ms")
    print(f"\n[Detalhes de Execução]")
    print(f"  - Sucessos:               {success_count}")
    print(f"  - Aborts (Erros Gerais):  {abort_count}")
    print(f"  - Deadlocks/Falhas Seriais:{deadlock_count}")
    print(f"  - Taxa Total de Aborts:   {abort_rate_percent:.2f}%")
    print(f"\n[Estatísticas por Transação]")
    grouped_stats = df.groupby('name').agg(
        total_execucoes=('name', 'count'),
        tempo_medio_ms=('duration', lambda x: x.mean() * 1000),
        sucessos=('status', lambda x: (x == 'SUCCESS').sum()),
        aborts=('status', lambda x: (x == 'ABORT').sum()),
        deadlocks=('status', lambda x: (x == 'DEADLOCK').sum())
    )
    print(grouped_stats.to_string())
    print("\n====================================================")

    generate_graphs(df)