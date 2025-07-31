import pyodbc
import random
import threading
import time
import queue
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# ==============================================================================
# --- CONFIGURAÇÕES DO TESTE E DO BANCO DE DADOS ---
# ==============================================================================

# Parâmetros do Teste
NUM_WORKERS = 4                # Número de threads/usuários simultâneos
TEST_DURATION_SECONDS = 120    # Duração do teste em segundos

# --- CONFIGURAÇÕES DO SQL SERVER ---

DB_CONFIG = {
    "server": "localhost,1434",
    "user": "sa",
    "password": "SuaSenhaForte#1234",
    "driver": "{ODBC Driver 17 for SQL Server}"
}

# --- CONFIGURAÇÕES DA RESTAURAÇÃO ---
DB_TEST_NAME = "TPCH_Test"
BACKUP_FILE_PATH_IN_CONTAINER = '/var/opt/mssql/data/BACKUP_SQLSERVER2005_TPCH.BAK'

LOGICAL_DATA_NAME = 'TPC-H_Data'
LOGICAL_LOG_NAME = 'TPC-H_Log'


# Filas para comunicação entre threads
task_queue = queue.Queue()
results_queue = queue.Queue()


# ==============================================================================
# --- QUERIES DO TPC-H ---
# ==============================================================================


TPCH_QUERIES = [
    (1, "select l_returnflag,l_linestatus,sum(l_quantity) as sum_qty,sum(l_extendedprice) as sum_base_price,sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,avg(l_quantity) as avg_qty,avg(l_extendedprice) as avg_price,avg(l_discount) as avg_disc,count(*) as count_order from lineitem where l_shipdate <= '1998-11-30' group by l_returnflag, l_linestatus order by l_returnflag, l_linestatus"),
    (2, "select s_acctbal, s_name,n_name,p_partkey,p_mfgr,s_address,s_phone,s_comment from part, supplier, partsupp, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 20 and p_type like '%COPPER' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AMERICA' and ps_supplycost = (select min(ps_supplycost) from partsupp, supplier, nation, region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AMERICA') order by s_acctbal desc,n_name,s_name, p_partkey"),
    (3, "select l_orderkey,sum(l_extendedprice * (1 - l_discount)) as revenue,o_orderdate,o_shippriority from customer, orders, lineitem where c_mktsegment = 'AUTOMOBILE' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1998-12-31' and l_shipdate > '1991-01-01' group by l_orderkey,o_orderdate,o_shippriority order by revenue desc,o_orderdate"),
    (4, "select o_orderpriority,count(*) as order_count from orders where o_orderdate >= '1998-08-01' and o_orderdate < '1998-11-08' and exists (select * from lineitem where l_orderkey = o_orderkey and l_commitdate < l_receiptdate) group by o_orderpriority order by o_orderpriority"),
    (5, "select n_name,sum(l_extendedprice * (1 - l_discount)) as revenue from customer, orders, lineitem, supplier, nation, region where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey and c_nationkey = s_nationkey and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'AMERICA' and o_orderdate >= '1991-08-01' and o_orderdate < '1992-08-01' group by n_name order by revenue desc"),
    (6, "select sum(l_extendedprice * l_discount) as revenue from lineitem where l_shipdate >= '1998-01-07' and l_shipdate < '1999-01-07' and l_discount between 2- 0.01 and 2 + 0.01 and l_quantity < 5"),
    (7, "select supp_nation,cust_nation,l_year,sum(volume) as revenue from (select n1.n_name as supp_nation,n2.n_name as cust_nation,datepart(year,l_shipdate) as l_year,l_extendedprice * (1 - l_discount) as volume from supplier, lineitem, orders, customer, nation n1, nation n2 where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey and ((n1.n_name = 'ARGENTINA' and n2.n_name = 'ARGENTINA') or (n1.n_name = 'BRAZIL' and n2.n_name = 'BRAZIL')) and l_shipdate between '1995-01-01' and '1996-12-31') as shipping group by supp_nation,cust_nation,l_year order by supp_nation,cust_nation,l_year"),
    (8, "select o_year,sum(case when nation = 'UNITED STATES' then volume else 0 end) / sum(volume) as mkt_share from (select datepart(year, o_orderdate) as o_year,l_extendedprice * (1 - l_discount) as volume,n2.n_name as nation from part, supplier, lineitem, orders, customer, nation n1, nation n2, region where p_partkey = l_partkey and s_suppkey = l_suppkey and l_orderkey = o_orderkey and o_custkey = c_custkey and c_nationkey = n1.n_nationkey and n1.n_regionkey = r_regionkey and r_name = 'AFRICA' and s_nationkey = n2.n_nationkey and o_orderdate between '1995-01-01' and '1996-12-31' and p_type = 'ECONOMY BRUSHED COPPER') as all_nations group by o_year order by o_year"),
    (9, "select nation,o_year,sum(amount) as sum_profit from (select n_name as nation,datepart(year,o_orderdate) as o_year,l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount from part, supplier, lineitem, partsupp, orders, nation where s_suppkey = l_suppkey and ps_suppkey = l_suppkey and ps_partkey = l_partkey and p_partkey = l_partkey and o_orderkey = l_orderkey and s_nationkey = n_nationkey and p_name like '%blush%' ) as profit group by nation,o_year order by nation,o_year desc"),
    (10, "select c_custkey,c_name,sum(l_extendedprice * (1 - l_discount)) as revenue,c_acctbal,n_name,c_address,c_phone,c_comment from customer, orders, lineitem, nation where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= '1992-08-01' and o_orderdate < '1992-11-01' and l_returnflag = 'R' and c_nationkey = n_nationkey group by c_custkey,c_name,c_acctbal,c_phone,n_name,c_address,c_comment order by revenue desc"),
    (11, "select ps_partkey,sum(ps_supplycost * ps_availqty) as value from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'BRAZIL' group by ps_partkey having sum(ps_supplycost * ps_availqty) > (select sum(ps_supplycost * ps_availqty) * 2 from partsupp, supplier, nation where ps_suppkey = s_suppkey and s_nationkey = n_nationkey and n_name = 'BRAZIL' ) order by value desc"),
    (12, "select l_shipmode,sum(case when o_orderpriority = '1-URGENT' or o_orderpriority = '2-HIGH' then 1 else 0 end) as high_line_count,sum(case when o_orderpriority <> '1-URGENT' and o_orderpriority <> '2-HIGH' then 1 else 0 end) as low_line_count from orders, lineitem where o_orderkey = l_orderkey and l_shipmode in ('TRUCK', 'AIR') and l_commitdate < l_receiptdate and l_shipdate < l_commitdate and l_receiptdate >= '1996-01-01' and l_receiptdate < '1997-01-01' group by l_shipmode order by l_shipmode"),
    (13, "select c_count,count(*) as custdist from (select c_custkey,count(o_orderkey) from customer left outer join orders on c_custkey = o_custkey and o_comment not like '%even%deposits%' group by c_custkey ) as c_orders (c_custkey, c_count) group by c_count order by custdist desc,c_count desc"),
    (14, "select 100.00 * sum(case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue from lineitem, part where l_partkey = p_partkey and l_shipdate >= '1996-02-01' and l_shipdate < '1996-03-01'"),
    (15, "select p_brand,p_type,p_size,count(distinct ps_suppkey) as supplier_cnt from partsupp, part where p_partkey = ps_partkey and p_brand <> 'Brand#13' and p_type not like 'STANDARD%' and p_size in (7, 12, 14, 16, 21, 23, 32, 43) and ps_suppkey not in (select s_suppkey from supplier where s_comment like '%Customer%Complaints%' ) group by p_brand,p_type,p_size order by supplier_cnt desc,p_brand,p_type,p_size"),
    (16, "select sum(l_extendedprice) / 7.0 as avg_yearly from lineitem, part where p_partkey = l_partkey and p_brand = 'Brand#13' and p_container = 'JUMBO PKG' and l_quantity < (select 0.2 * avg(l_quantity) from lineitem where l_partkey = p_partkey )"),
    (17, "select c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum(l_quantity) from customer, orders, lineitem where o_orderkey in (select l_orderkey from lineitem group by l_orderkey having sum(l_quantity) > 3 ) and c_custkey = o_custkey and o_orderkey = l_orderkey group by c_name, c_custkey,o_orderkey,o_orderdate,o_totalprice order by o_totalprice desc,o_orderdate"),
    (18, "select sum(l_extendedprice* (1 - l_discount)) as revenue from lineitem, part where ( p_partkey = l_partkey and p_brand = 'Brand#13' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and l_quantity >= 4 and l_quantity <= 14 and p_size between 1 and 5 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#44' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and l_quantity >= 5 and l_quantity <= 15 and p_size between 1 and 10 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' ) or ( p_partkey = l_partkey and p_brand = 'Brand#53' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and l_quantity >= 6 and l_quantity <= 16 and p_size between 1 and 15 and l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct = 'DELIVER IN PERSON' )"),
    (19, "select s_name,s_address from supplier, nation where s_suppkey in (select distinct (ps_suppkey) from partsupp, part where ps_partkey=p_partkey and p_name like 'dim%' and ps_availqty > ( select 0.5 * sum(l_quantity) from lineitem where l_partkey = ps_partkey and l_suppkey = ps_suppkey and l_shipdate >= '1997-03-01' and l_shipdate < '1998-03-01' ) ) and s_nationkey = n_nationkey and n_name = 'ARGENTINA' order by s_name"),
    (20, "select s_name, count(*) as numwait from supplier, lineitem l1, orders, nation where s_suppkey = l1.l_suppkey and o_orderkey = l1.l_orderkey and o_orderstatus = 'F' and l1.l_receiptdate > l1.l_commitdate and exists ( select * from lineitem l2 where l2.l_orderkey = l1.l_orderkey and l2.l_suppkey <> l1.l_suppkey ) and not exists ( select * from lineitem l3 where l3.l_orderkey = l1.l_orderkey and l3.l_suppkey <> l1.l_suppkey and l3.l_receiptdate > l3.l_commitdate ) and s_nationkey = n_nationkey and n_name = 'BRAZIL' group by s_name order by numwait desc, s_name"),
    (21, "select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal from ( select substring(c_phone, 1, 2) as cntrycode, c_acctbal from customer where substring(c_phone, 1, 2) in ('25', '11', '13', '14', '30', '23', '18') and c_acctbal > ( select avg(c_acctbal) from customer where c_acctbal > 0.00 and substring(c_phone, 1, 2) in ('25', '11', '13', '14', '30', '23', '18') ) and not exists ( select * from orders where o_custkey = c_custkey ) ) as vip group by cntrycode order by cntrycode"),
    (22, "select s_suppkey, s_name, s_address, s_phone, total_revenue from supplier, h_revenue where s_suppkey = supplier_no and total_revenue = ( select max(total_revenue) from h_revenue ) order by s_suppkey"),
]
# ==============================================================================
# --- ROTINA DE RESET DO BANCO DE DADOS ---
# ==============================================================================
def reset_test_database():
    print("="*60)
    print(f"--- RESETANDO O AMBIENTE DE TESTE DO SQL SERVER ---")
    print(f"Alvo: Restaurar '{DB_TEST_NAME}' a partir de '{BACKUP_FILE_PATH_IN_CONTAINER}'")

    admin_conn_str = f"DRIVER={DB_CONFIG['driver']};SERVER={DB_CONFIG['server']};DATABASE=master;UID={DB_CONFIG['user']};PWD={DB_CONFIG['password']}"
    conn = None
    try:
        conn = pyodbc.connect(admin_conn_str, autocommit=True)
        cursor = conn.cursor()

        print(f"1. Derrubando banco de dados '{DB_TEST_NAME}' (se existir)...")
        if cursor.execute(f"SELECT db_id('{DB_TEST_NAME}')").fetchval() is not None:
             cursor.execute(f"ALTER DATABASE [{DB_TEST_NAME}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
             cursor.execute(f"DROP DATABASE [{DB_TEST_NAME}]")

        print(f"2. Restaurando banco de dados a partir do backup...")
        restore_command = f"""
        RESTORE DATABASE [{DB_TEST_NAME}]
        FROM DISK = '{BACKUP_FILE_PATH_IN_CONTAINER}'
        WITH
           MOVE '{LOGICAL_DATA_NAME}' TO '/var/opt/mssql/data/{DB_TEST_NAME}.mdf',
           MOVE '{LOGICAL_LOG_NAME}'  TO '/var/opt/mssql/data/{DB_TEST_NAME}_log.ldf',
           REPLACE;
        """
        cursor.execute(restore_command)
        while cursor.nextset():
            pass
        
        print("--- AMBIENTE DE TESTE PRONTO ---")
        cursor.close()

    except pyodbc.Error as e:
        print(f"\nERRO FATAL: Falha ao resetar o banco de dados.")
        print("Verifique se o contêiner está em execução, se o arquivo .bak existe no caminho correto dentro dele")
        print("e se os NOMES LÓGICOS estão corretos no script.")
        print(f"Detalhe do erro: {e}")
        exit(1)
    finally:
        if conn:
            conn.close()
        print("="*60)


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

    def run(self):
        test_conn_str = f"DRIVER={DB_CONFIG['driver']};SERVER={DB_CONFIG['server']};DATABASE={DB_TEST_NAME};UID={DB_CONFIG['user']};PWD={DB_CONFIG['password']}"
        try:
            self.conn = pyodbc.connect(test_conn_str)
            cursor = self.conn.cursor()
        except pyodbc.Error as e:
            print(f"{self.name}: ERRO DE CONEXÃO - {e}")
            self.result_q.put(('CONNECTION_ERROR', 0, 'FATAL', 0))
            return

        while not self.stop_event.is_set():
            try:
                query_id, query_sql = self.task_q.get(timeout=0.1)
                start_time = time.monotonic()
                status = "SUCCESS"
                
                try:
                    cursor.execute(query_sql)
                    cursor.fetchall()
                    self.conn.commit()
                except pyodbc.Error as e:
                    self.conn.rollback()
                    status = "ABORT"
                
                end_time = time.monotonic()
                duration = end_time - start_time
                completion_timestamp = end_time - self.test_start_time
                self.result_q.put((f"Query {query_id}", duration, status, completion_timestamp))
                self.task_q.task_done()
            except queue.Empty:
                if self.stop_event.is_set():
                    break
                continue
        
        if self.conn:
            cursor.close()
            self.conn.close()

    def stop(self):
        self.stop_event.set()

# ==============================================================================
# --- FUNÇÃO DE GERAÇÃO DE RELATÓRIO E GRÁFICOS ---
# ==============================================================================
def generate_report_and_graphs(results_df, duration):
    if results_df.empty:
        print("Nenhum resultado foi coletado para gerar o relatório.")
        return
    
    total_transactions = len(results_df)
    throughput = total_transactions / duration if duration > 0 else 0
    success_count = len(results_df[results_df['status'] == 'SUCCESS'])
    abort_count = len(results_df[results_df['status'] == 'ABORT'])
    avg_response_time_ms = (results_df['duration'].sum() / total_transactions * 1000) if total_transactions > 0 else 0
    error_rate_percent = (abort_count / total_transactions * 100) if total_transactions > 0 else 0

    print("\n\n" + "="*20 + " RELATÓRIO FINAL DO TESTE TPC-H (SQL Server) " + "="*20)
    print(f"\n[ Métricas Gerais ]")
    print(f"  - Duração do Teste:       {duration:.2f} segundos")
    print(f"  - Total de Transações:    {total_transactions}")
    print(f"  - Vazão (Throughput):     {throughput:.2f} transações/segundo")
    print(f"  - Tempo de Resposta Médio:{avg_response_time_ms:.2f} ms")
    print(f"  - Taxa Total de Erros:    {error_rate_percent:.2f}%")
    print(f"\n[ Estatísticas por Query ]")
    grouped_stats = results_df.groupby('name').agg(
        Execucoes=('name', 'count'),
        Tempo_Medio_ms=('duration', lambda x: x.mean() * 1000),
        Sucessos=('status', lambda x: (x == 'SUCCESS').sum()),
        Aborts=('status', lambda x: (x == 'ABORT').sum()),
    )
    print(grouped_stats.to_string(float_format="%.2f"))
    print("\n" + "="*70)

    print("\nGerando gráficos...")
    sns.set_theme(style="whitegrid")
    fig1, ax1 = plt.subplots(figsize=(12, 6))
    results_df['second'] = results_df['timestamp'].astype(int)
    throughput_data = results_df[results_df['second'] < duration].groupby('second').size()
    if not throughput_data.empty:
        sns.lineplot(x=throughput_data.index, y=throughput_data.values, ax=ax1, color='crimson', lw=2)
    ax1.set_title('Vazão Durante o Teste TPC-H (SQL Server)', fontsize=16)
    ax1.set_xlabel('Tempo (segundos)', fontsize=12)
    ax1.set_ylabel('Vazão (Queries por Segundo)', fontsize=12)
    fig1.tight_layout()
    fig1.savefig('grafico_vazao_tpch_sqlserver.png', dpi=150)
    plt.close(fig1)

    fig2, ax2 = plt.subplots(figsize=(10, 10))
    avg_response_times = results_df.groupby('name')['duration'].mean() * 1000
    avg_response_times = avg_response_times.sort_values(ascending=False)
    sns.barplot(x=avg_response_times.values, y=avg_response_times.index, ax=ax2, palette='magma')
    ax2.set_title('Tempo de Resposta Médio por Query TPC-H (SQL Server)', fontsize=16)
    ax2.set_xlabel('Tempo de Resposta (ms)', fontsize=12)
    ax2.set_ylabel('Query', fontsize=12)
    ax2.bar_label(ax2.containers[0], fmt='%.0f ms', padding=3)
    fig2.tight_layout()
    fig2.savefig('grafico_tempo_resposta_tpch_sqlserver.png', dpi=150)
    plt.close(fig2)
    
    print("Gráficos salvos como 'grafico_vazao_tpch_sqlserver.png' e 'grafico_tempo_resposta_tpch_sqlserver.png'")


# ==============================================================================
# --- BLOCO DE EXECUÇÃO PRINCIPAL ---
# ==============================================================================
# ==============================================================================
# --- BLOCO DE EXECUÇÃO PRINCIPAL (MODELO HÍBRIDO) ---
# ==============================================================================
if __name__ == "__main__":
    #reset_test_database()
    
    print(f"\n--- INICIANDO TESTE DE CARGA TPC-H (MODELO HÍBRIDO) ---")
    print(f"Workers: {NUM_WORKERS} | Duração Máxima: {TEST_DURATION_SECONDS} segundos\n")
    
    test_start_time = time.monotonic()
    
    workers = []
    for i in range(NUM_WORKERS):
        worker = WorkerThread(f"Worker-{i+1}", task_queue, results_queue, test_start_time)
        worker.start()
        workers.append(worker)

    # --- FASE 1: EXECUÇÃO GARANTIDA DE TODAS AS QUERIES ---
    print("--- Fase 1: Executando cada query pelo menos uma vez... ---")
    guaranteed_tasks = TPCH_QUERIES.copy()
    for task in guaranteed_tasks:
        task_queue.put(task)
    
    # Espera apenas o lote inicial e obrigatório ser concluído
    task_queue.join()
    
    phase1_duration = time.monotonic() - test_start_time
    print(f"--- Fase 1 concluída em {phase1_duration:.2f} segundos. ---")

    # --- FASE 2: CARGA ALEATÓRIA NO TEMPO RESTANTE ---
    if phase1_duration < TEST_DURATION_SECONDS:
        print(f"--- Fase 2: Iniciando carga aleatória pelo tempo restante... ---")
        query_pool = []
        while time.monotonic() - test_start_time < TEST_DURATION_SECONDS:
            if not query_pool:
                query_pool = TPCH_QUERIES.copy()
                random.shuffle(query_pool)
            
            task = query_pool.pop()
            task_queue.put(task)
            time.sleep(0.001)

    print(f"\n--- Tempo de teste esgotado ou carga concluída. Finalizando workers... ---")
    for worker in workers:
        worker.stop()
    for worker in workers:
        worker.join()

    print("--- Coletando e processando resultados... ---")
    results = []
    while not results_queue.empty():
        results.append(results_queue.get())

    if not results:
        print("Nenhum resultado foi coletado. Encerrando.")
        exit()

    df = pd.DataFrame(results, columns=['name', 'duration', 'status', 'timestamp'])
    
    # O relatório final agora considerará todas as queries executadas
    # A duração do relatório será a duração real total do teste
    final_duration = time.monotonic() - test_start_time
    
    generate_report_and_graphs(df, final_duration)