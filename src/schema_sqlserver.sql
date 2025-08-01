-- Esquema TPC-E convertido para SQL Server (T-SQL)

CREATE TABLE e_account_permission (
    ap_ca_id DECIMAL(11,0),
    ap_acl CHAR(4),
    ap_tax_id CHAR(20),
    ap_l_name CHAR(30),
    ap_f_name CHAR(30)
);

CREATE TABLE e_account_permission_bak (
    ap_ca_id DECIMAL(11,0),
    ap_acl CHAR(4),
    ap_tax_id CHAR(20),
    ap_l_name CHAR(30),
    ap_f_name CHAR(30)
);

CREATE TABLE e_address (
    ad_id DECIMAL(11,0),
    ad_line1 CHAR(80),
    ad_line2 CHAR(80),
    ad_zc_code CHAR(12),
    ad_ctry CHAR(80)
);

CREATE TABLE e_address_bak (
    ad_id DECIMAL(11,0),
    ad_line1 CHAR(80),
    ad_line2 CHAR(80),
    ad_zc_code CHAR(12),
    ad_ctry CHAR(80)
);

CREATE TABLE e_broker (
    b_id DECIMAL(11,0),
    b_st_id CHAR(4),
    b_name CHAR(100),
    b_num_trades DECIMAL(9,0),
    b_comm_total DECIMAL(12,2)
);

CREATE TABLE e_broker_bak (
    b_id DECIMAL(11,0),
    b_st_id CHAR(4),
    b_name CHAR(100),
    b_num_trades DECIMAL(9,0),
    b_comm_total DECIMAL(12,2)
);

CREATE TABLE e_cash_transaction_bak (
    ct_t_id DECIMAL(15,0),
    ct_dts DATETIME2,
    ct_amt DECIMAL(10,2),
    ct_name CHAR(100)
);

CREATE TABLE e_charge (
    ch_tt_id CHAR(3),
    ch_c_tier DECIMAL(1,0),
    ch_chrg DECIMAL(10,2)
);

CREATE TABLE e_charge_bak (
    ch_tt_id CHAR(3),
    ch_c_tier DECIMAL(1,0),
    ch_chrg DECIMAL(10,2)
);

CREATE TABLE e_commission_rate (
    cr_c_tier DECIMAL(1,0),
    cr_tt_id CHAR(3),
    cr_ex_id CHAR(6),
    cr_from_qty DECIMAL(9,0),
    cr_to_qty DECIMAL(9,0),
    cr_rate DECIMAL(5,2)
);

CREATE TABLE e_commission_rate_bak (
    cr_c_tier DECIMAL(1,0),
    cr_tt_id CHAR(3),
    cr_ex_id CHAR(6),
    cr_from_qty DECIMAL(9,0),
    cr_to_qty DECIMAL(9,0),
    cr_rate DECIMAL(5,2)
);

CREATE TABLE e_company (
    co_id DECIMAL(11,0),
    co_st_id CHAR(4),
    co_name CHAR(60),
    co_in_id CHAR(2),
    co_sp_rate CHAR(4),
    co_ceo CHAR(100),
    co_ad_id DECIMAL(11,0),
    co_desc CHAR(150),
    co_open_date DATETIME2
);

CREATE TABLE e_company_bak (
    co_id DECIMAL(11,0),
    co_st_id CHAR(4),
    co_name CHAR(60),
    co_in_id CHAR(2),
    co_sp_rate CHAR(4),
    co_ceo CHAR(100),
    co_ad_id DECIMAL(11,0),
    co_desc CHAR(150),
    co_open_date DATETIME2
);

CREATE TABLE e_company_competitor (
    cp_co_id DECIMAL(11,0),
    cp_comp_co_id DECIMAL(11,0),
    cp_in_id CHAR(2)
);

CREATE TABLE e_company_competitor_bak (
    cp_co_id DECIMAL(11,0),
    cp_comp_co_id DECIMAL(11,0),
    cp_in_id CHAR(2)
);

CREATE TABLE e_customer (
    c_id DECIMAL(11,0),
    c_tax_id CHAR(20),
    c_st_id CHAR(4),
    c_l_name CHAR(30),
    c_f_name CHAR(30),
    c_m_name CHAR(1),
    c_gndr CHAR(1),
    c_tier DECIMAL(1,0),
    c_dob DATETIME2,
    c_ad_id DECIMAL(11,0),
    c_ctry_1 CHAR(3),
    c_area_1 CHAR(3),
    c_local_1 CHAR(10),
    c_ext_1 CHAR(5),
    c_ctry_2 CHAR(3),
    c_area_2 CHAR(3),
    c_local_2 CHAR(10),
    c_ext_2 CHAR(5),
    c_ctry_3 CHAR(3),
    c_area_3 CHAR(3),
    c_local_3 CHAR(10),
    c_ext_3 CHAR(5),
    c_email_1 CHAR(50),
    c_email_2 CHAR(50)
);

CREATE TABLE e_customer_account (
    ca_id DECIMAL(11,0),
    ca_b_id DECIMAL(11,0),
    ca_c_id DECIMAL(11,0),
    ca_name CHAR(50),
    ca_tax_st DECIMAL(1,0),
    ca_bal DECIMAL(12,2)
);

CREATE TABLE e_customer_account_bak (
    ca_id DECIMAL(11,0),
    ca_b_id DECIMAL(11,0),
    ca_c_id DECIMAL(11,0),
    ca_name CHAR(50),
    ca_tax_st DECIMAL(1,0),
    ca_bal DECIMAL(12,2)
);

CREATE TABLE e_customer_bak (
    c_id DECIMAL(11,0),
    c_tax_id CHAR(20),
    c_st_id CHAR(4),
    c_l_name CHAR(30),
    c_f_name CHAR(30),
    c_m_name CHAR(1),
    c_gndr CHAR(1),
    c_tier DECIMAL(1,0),
    c_dob DATETIME2,
    c_ad_id DECIMAL(11,0),
    c_ctry_1 CHAR(3),
    c_area_1 CHAR(3),
    c_local_1 CHAR(10),
    c_ext_1 CHAR(5),
    c_ctry_2 CHAR(3),
    c_area_2 CHAR(3),
    c_local_2 CHAR(10),
    c_ext_2 CHAR(5),
    c_ctry_3 CHAR(3),
    c_area_3 CHAR(3),
    c_local_3 CHAR(10),
    c_ext_3 CHAR(5),
    c_email_1 CHAR(50),
    c_email_2 CHAR(50)
);

CREATE TABLE e_customer_taxrate (
    cx_tx_id CHAR(4),
    cx_c_id DECIMAL(11,0)
);

CREATE TABLE e_customer_taxrate_bak (
    cx_tx_id CHAR(4),
    cx_c_id DECIMAL(11,0)
);

CREATE TABLE e_daily_market (
    dm_date DATETIME2,
    dm_s_symb CHAR(15),
    dm_close DECIMAL(8,2),
    dm_high DECIMAL(8,2),
    dm_low DECIMAL(8,2),
    dm_vol DECIMAL(12,0)
);

CREATE TABLE e_daily_market_bak (
    dm_date DATETIME2,
    dm_s_symb CHAR(15),
    dm_close DECIMAL(8,2),
    dm_high DECIMAL(8,2),
    dm_low DECIMAL(8,2),
    dm_vol DECIMAL(12,0)
);

CREATE TABLE e_exchange (
    ex_id CHAR(6),
    ex_name CHAR(100),
    ex_num_symb DECIMAL(6,0),
    ex_open DECIMAL(4,0),
    ex_close DECIMAL(4,0),
    ex_desc CHAR(150),
    ex_ad_id DECIMAL(11,0)
);

CREATE TABLE e_exchange_bak (
    ex_id CHAR(6),
    ex_name CHAR(100),
    ex_num_symb DECIMAL(6,0),
    ex_open DECIMAL(4,0),
    ex_close DECIMAL(4,0),
    ex_desc CHAR(150),
    ex_ad_id DECIMAL(11,0)
);

CREATE TABLE e_financial (
    fi_co_id DECIMAL(11,0),
    fi_year DECIMAL(4,0),
    fi_qtr DECIMAL(1,0),
    fi_qtr_start_date DATETIME2,
    fi_revenue DECIMAL(15,2),
    fi_net_earn DECIMAL(15,2),
    fi_basic_eps DECIMAL(10,2),
    fi_dilut_eps DECIMAL(10,2),
    fi_margin DECIMAL(10,2),
    fi_inventory DECIMAL(15,2),
    fi_assets DECIMAL(15,2),
    fi_liability DECIMAL(15,2),
    fi_out_basic DECIMAL(12,0),
    fi_out_dilut DECIMAL(12,0)
);

CREATE TABLE e_financial_bak (
    fi_co_id DECIMAL(11,0),
    fi_year DECIMAL(4,0),
    fi_qtr DECIMAL(1,0),
    fi_qtr_start_date DATETIME2,
    fi_revenue DECIMAL(15,2),
    fi_net_earn DECIMAL(15,2),
    fi_basic_eps DECIMAL(10,2),
    fi_dilut_eps DECIMAL(10,2),
    fi_margin DECIMAL(10,2),
    fi_inventory DECIMAL(15,2),
    fi_assets DECIMAL(15,2),
    fi_liability DECIMAL(15,2),
    fi_out_basic DECIMAL(12,0),
    fi_out_dilut DECIMAL(12,0)
);

CREATE TABLE e_holding (
    h_t_id DECIMAL(15,0) NOT NULL,
    h_ca_id DECIMAL(11,0) NOT NULL,
    h_s_symb CHAR(15) NOT NULL,
    h_dts DATETIME2 NOT NULL,
    h_price DECIMAL(8,2) NOT NULL,
    h_qty DECIMAL(9,0) NOT NULL
);

CREATE TABLE e_holding_bak (
    h_t_id DECIMAL(15,0),
    h_ca_id DECIMAL(11,0),
    h_s_symb CHAR(15),
    h_dts DATETIME2,
    h_price DECIMAL(8,2),
    h_qty DECIMAL(9,0)
);

CREATE TABLE e_holding_history_bak (
    hh_h_t_id DECIMAL(15,0),
    hh_t_id DECIMAL(15,0),
    hh_before_qty DECIMAL(9,0),
    hh_after_qty DECIMAL(9,0)
);

CREATE TABLE e_holding_summary (
    hs_ca_id DECIMAL(11,0) NOT NULL,
    hs_s_symb CHAR(15) NOT NULL,
    hs_qty DECIMAL(9,0) NOT NULL
);

CREATE TABLE e_holding_summary_bak (
    hs_ca_id DECIMAL(11,0),
    hs_s_symb CHAR(15),
    hs_qty DECIMAL(9,0)
);

CREATE TABLE e_industry (
    in_id CHAR(2),
    in_name CHAR(50),
    in_sc_id CHAR(2)
);

CREATE TABLE e_industry_bak (
    in_id CHAR(2),
    in_name CHAR(50),
    in_sc_id CHAR(2)
);

CREATE TABLE e_last_trade (
    lt_s_symb CHAR(15),
    lt_dts DATETIME2,
    lt_price DECIMAL(8,2),
    lt_open_price DECIMAL(8,2),
    lt_vol DECIMAL(12,0)
);

CREATE TABLE e_last_trade_bak (
    lt_s_symb CHAR(15),
    lt_dts DATETIME2,
    lt_price DECIMAL(8,2),
    lt_open_price DECIMAL(8,2),
    lt_vol DECIMAL(12,0)
);

CREATE TABLE e_news_item (
    ni_id DECIMAL(11,0),
    ni_headline CHAR(80),
    ni_summary CHAR(254),
    ni_item NVARCHAR(MAX),
    ni_dts DATETIME2,
    ni_source CHAR(30),
    ni_author CHAR(30)
);

CREATE TABLE e_news_item_bak (
    ni_id DECIMAL(11,0),
    ni_headline CHAR(80),
    ni_summary CHAR(254),
    ni_item NVARCHAR(MAX),
    ni_dts DATETIME2,
    ni_source CHAR(30),
    ni_author CHAR(30)
);

CREATE TABLE e_news_xref (
    nx_ni_id DECIMAL(11,0),
    nx_co_id DECIMAL(11,0)
);

CREATE TABLE e_news_xref_bak (
    nx_ni_id DECIMAL(11,0),
    nx_co_id DECIMAL(11,0)
);

CREATE TABLE e_sector (
    sc_id CHAR(2),
    sc_name CHAR(30)
);

CREATE TABLE e_sector_bak (
    sc_id CHAR(2),
    sc_name CHAR(30)
);

CREATE TABLE e_security (
    s_symb CHAR(15),
    s_issue CHAR(6),
    s_st_id CHAR(4),
    s_name CHAR(70),
    s_ex_id CHAR(6),
    s_co_id DECIMAL(11,0),
    s_num_out DECIMAL(12,0),
    s_start_date DATETIME2,
    s_exch_date DATETIME2,
    s_pe DECIMAL(10,2),
    s_52wk_high DECIMAL(8,2),
    s_52wk_high_date DATETIME2,
    s_52wk_low DECIMAL(8,2),
    s_52wk_low_date DATETIME2,
    s_dividend DECIMAL(10,2),
    s_yield DECIMAL(5,2)
);

CREATE TABLE e_security_bak (
    s_symb CHAR(15),
    s_issue CHAR(6),
    s_st_id CHAR(4),
    s_name CHAR(70),
    s_ex_id CHAR(6),
    s_co_id DECIMAL(11,0),
    s_num_out DECIMAL(12,0),
    s_start_date DATETIME2,
    s_exch_date DATETIME2,
    s_pe DECIMAL(10,2),
    s_52wk_high DECIMAL(8,2),
    s_52wk_high_date DATETIME2,
    s_52wk_low DECIMAL(8,2),
    s_52wk_low_date DATETIME2,
    s_dividend DECIMAL(10,2),
    s_yield DECIMAL(5,2)
);

CREATE TABLE e_settlement_bak (
    se_t_id DECIMAL(15,0),
    se_cash_type CHAR(40),
    se_cash_due_date DATETIME2,
    se_amt DECIMAL(10,2)
);

CREATE TABLE e_status_type (
    st_id CHAR(4),
    st_name CHAR(10)
);

CREATE TABLE e_status_type_bak (
    st_id CHAR(4),
    st_name CHAR(10)
);

CREATE TABLE e_taxrate (
    tx_id CHAR(4),
    tx_name CHAR(50),
    tx_rate DECIMAL(6,5)
);

CREATE TABLE e_taxrate_bak (
    tx_id CHAR(4),
    tx_name CHAR(50),
    tx_rate DECIMAL(6,5)
);

CREATE TABLE e_trade_bak (
    t_id DECIMAL(15,0),
    t_dts DATETIME2,
    t_st_id CHAR(4),
    t_tt_id CHAR(3),
    t_is_cash CHAR(1),
    t_s_symb CHAR(15),
    t_qty DECIMAL(9,0),
    t_bid_price DECIMAL(8,2),
    t_ca_id DECIMAL(11,0),
    t_exec_name CHAR(64),
    t_trade_price DECIMAL(8,2),
    t_chrg DECIMAL(10,2),
    t_comm DECIMAL(10,2),
    t_tax DECIMAL(10,2),
    t_lifo CHAR(1)
);

CREATE TABLE e_trade_history_bak (
    th_t_id DECIMAL(15,0),
    th_dts DATETIME2,
    th_st_id CHAR(4)
);

CREATE TABLE e_trade_request (
    tr_t_id DECIMAL(15,0),
    tr_tt_id CHAR(3),
    tr_s_symb CHAR(15),
    tr_qty DECIMAL(9,0),
    tr_bid_price DECIMAL(8,2),
    tr_b_id DECIMAL(11,0)
);

CREATE TABLE e_trade_request_bak (
    tr_t_id DECIMAL(15,0),
    tr_tt_id CHAR(3),
    tr_s_symb CHAR(15),
    tr_qty DECIMAL(9,0),
    tr_bid_price DECIMAL(8,2),
    tr_b_id DECIMAL(11,0)
);

CREATE TABLE e_trade_type (
    tt_id CHAR(3),
    tt_name CHAR(12),
    tt_is_sell CHAR(1),
    tt_is_mrkt CHAR(1)
);

CREATE TABLE e_trade_type_bak (
    tt_id CHAR(3),
    tt_name CHAR(12),
    tt_is_sell CHAR(1),
    tt_is_mrkt CHAR(1)
);

CREATE TABLE e_watch_item (
    wi_wl_id DECIMAL(11,0),
    wi_s_symb CHAR(15)
);

CREATE TABLE e_watch_item_bak (
    wi_wl_id DECIMAL(11,0),
    wi_s_symb CHAR(15)
);

CREATE TABLE e_watch_list (
    wl_id DECIMAL(11,0),
    wl_c_id DECIMAL(11,0)
);

CREATE TABLE e_watch_list_bak (
    wl_id DECIMAL(11,0),
    wl_c_id DECIMAL(11,0)
);

CREATE TABLE e_zip_code (
    zc_code CHAR(12),
    zc_town CHAR(80),
    zc_div CHAR(80)
);

CREATE TABLE e_zip_code_bak (
    zc_code CHAR(12),
    zc_town CHAR(80),
    zc_div CHAR(80)
);

CREATE TABLE tpc_e_block_info (
    tablename NVARCHAR(30) NOT NULL,
    current_block INT NOT NULL,
    max_block INT NOT NULL
);

CREATE TABLE tpc_e_load_progress (
    tablename NVARCHAR(30) NOT NULL,
    version INT NOT NULL,
    setnumber INT NOT NULL,
    prop_name NVARCHAR(50) NOT NULL,
    prop_value NVARCHAR(255)
);

CREATE TABLE tpc_e_properties (
    prop_name NVARCHAR(50) NOT NULL,
    prop_value NVARCHAR(2000) NOT NULL
);