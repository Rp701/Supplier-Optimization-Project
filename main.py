{\rtf1\ansi\ansicpg1252\cocoartf2761
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fmodern\fcharset0 Courier;}
{\colortbl;\red255\green255\blue255;\red113\green171\blue89;\red255\green255\blue255;\red202\green202\blue202;
\red188\green135\blue185;\red194\green126\blue101;\red167\green197\blue151;\red88\green148\blue206;\red212\green212\blue212;
\red212\green213\blue154;\red141\green212\blue254;\red114\green185\blue255;\red67\green192\blue160;\red71\green137\blue205;
}
{\*\expandedcolortbl;;\cssrgb\c50939\c71716\c42215;\cssrgb\c100000\c100000\c100000\c0;\cssrgb\c83229\c83229\c83125;
\cssrgb\c78876\c61228\c77620;\cssrgb\c80778\c56830\c46925;\cssrgb\c71008\c80807\c65805;\cssrgb\c41371\c64935\c84491;\cssrgb\c86370\c86370\c86262;
\cssrgb\c86261\c86245\c66529;\cssrgb\c61361\c86489\c99746;\cssrgb\c51206\c77912\c100000;\cssrgb\c30610\c78876\c69022;\cssrgb\c33936\c61427\c84130;
}
\paperw11900\paperh16840\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\deftab720
\pard\pardeftab720\partightenfactor0

\f0\fs28 \cf2 \cb3 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 # ==============================================================================\cf4 \strokec4 \
\cf2 \strokec2 # SMART PROCUREMENT ENGINE - V4.4 (COMPLETE FORENSIC DATA)\cf4 \strokec4 \
\cf2 \strokec2 # ==============================================================================\cf4 \strokec4 \
\cf2 \strokec2 # Author: Gemini (Assistant)\cf4 \strokec4 \
\cf2 \strokec2 # Changelog v4.4: Added missing Challenger Currency & Supplier IDs in Drill-Down.\cf4 \strokec4 \
\cf2 \strokec2 # ==============================================================================\cf4 \strokec4 \
\
\pard\pardeftab720\partightenfactor0
\cf5 \strokec5 import\cf4 \strokec4  sqlite3\
\cf5 \strokec5 import\cf4 \strokec4  pandas \cf5 \strokec5 as\cf4 \strokec4  pd\
\cf5 \strokec5 import\cf4 \strokec4  requests\
\cf5 \strokec5 import\cf4 \strokec4  os\
\cf5 \strokec5 from\cf4 \strokec4  google.colab \cf5 \strokec5 import\cf4 \strokec4  files \
\
\pard\pardeftab720\partightenfactor0
\cf2 \strokec2 # ==============================================================================\cf4 \strokec4 \
\cf2 \strokec2 # 1. CONFIGURATION\cf4 \strokec4 \
\cf2 \strokec2 # ==============================================================================\cf4 \strokec4 \
DB_FILENAME = \cf6 \strokec6 'warehouse'\cf4 \strokec4  \
TARGET_BUILD_ID = \cf7 \strokec7 6\cf4 \strokec4 \
REPORT_FILENAME = \cf8 \strokec8 f\cf6 \strokec6 'Procurement_Analysis_Order_\cf9 \strokec9 \{\cf4 \strokec4 TARGET_BUILD_ID\cf9 \strokec9 \}\cf6 \strokec6 .xlsx'\cf4 \strokec4 \
FOREX_API_URL = \cf6 \strokec6 "https://api.exchangerate-api.com/v4/latest/EUR"\cf4 \strokec4 \
\
\cf2 \strokec2 # ==============================================================================\cf4 \strokec4 \
\cf2 \strokec2 # 2. CORE FUNCTIONS\cf4 \strokec4 \
\cf2 \strokec2 # ==============================================================================\cf4 \strokec4 \
\
\pard\pardeftab720\partightenfactor0
\cf8 \strokec8 def\cf4 \strokec4  \cf10 \strokec10 get_live_forex_factor\cf4 \strokec4 ()\cf9 \strokec9 :\cf4 \strokec4 \
    \cf5 \strokec5 try\cf9 \strokec9 :\cf4 \strokec4 \
        response = requests.get\cf9 \strokec9 (\cf4 \strokec4 FOREX_API_URL\cf9 \strokec9 )\cf4 \strokec4 \
        data = response.json\cf9 \strokec9 ()\cf4 \strokec4 \
        rate = data\cf9 \strokec9 [\cf6 \strokec6 'rates'\cf9 \strokec9 ][\cf6 \strokec6 'USD'\cf9 \strokec9 ]\cf4 \strokec4 \
        \cf10 \strokec10 print\cf9 \strokec9 (\cf8 \strokec8 f\cf6 \strokec6 "\uc0\u9989  Live Forex: 1 EUR = \cf9 \strokec9 \{\cf4 \strokec4 rate\cf9 \strokec9 \}\cf6 \strokec6  USD"\cf9 \strokec9 )\cf4 \strokec4 \
        \cf5 \strokec5 return\cf4 \strokec4  \cf7 \strokec7 1\cf4 \strokec4  / rate \
    \cf5 \strokec5 except\cf9 \strokec9 :\cf4 \strokec4 \
        \cf10 \strokec10 print\cf9 \strokec9 (\cf8 \strokec8 f\cf6 \strokec6 "\uc0\u9888 \u65039  Forex Error. Using fallback: 0.95"\cf9 \strokec9 )\cf4 \strokec4 \
        \cf5 \strokec5 return\cf4 \strokec4  \cf7 \strokec7 0.95\cf4 \strokec4 \
\
\cf8 \strokec8 def\cf4 \strokec4  \cf10 \strokec10 connect_db\cf4 \strokec4 (\cf11 \strokec11 db_path\cf4 \strokec4 )\cf9 \strokec9 :\cf4 \strokec4 \
    \cf5 \strokec5 if\cf4 \strokec4  \cf12 \strokec12 not\cf4 \strokec4  os.path.exists\cf9 \strokec9 (\cf4 \strokec4 db_path\cf9 \strokec9 ):\cf4 \strokec4 \
        \cf5 \strokec5 if\cf4 \strokec4  os.path.exists\cf9 \strokec9 (\cf4 \strokec4 db_path + \cf6 \strokec6 ".sqlite"\cf9 \strokec9 ):\cf4 \strokec4  \cf5 \strokec5 return\cf4 \strokec4  sqlite3.connect\cf9 \strokec9 (\cf4 \strokec4 db_path + \cf6 \strokec6 ".sqlite"\cf9 \strokec9 )\cf4 \strokec4 \
        \cf5 \strokec5 if\cf4 \strokec4  os.path.exists\cf9 \strokec9 (\cf4 \strokec4 db_path + \cf6 \strokec6 ".db"\cf9 \strokec9 ):\cf4 \strokec4  \cf5 \strokec5 return\cf4 \strokec4  sqlite3.connect\cf9 \strokec9 (\cf4 \strokec4 db_path + \cf6 \strokec6 ".db"\cf9 \strokec9 )\cf4 \strokec4 \
        \cf10 \strokec10 print\cf9 \strokec9 (\cf8 \strokec8 f\cf6 \strokec6 "\uc0\u10060  ERROR: File '\cf9 \strokec9 \{\cf4 \strokec4 db_path\cf9 \strokec9 \}\cf6 \strokec6 ' not found! Check left folder panel."\cf9 \strokec9 )\cf4 \strokec4 \
        \cf5 \strokec5 raise\cf4 \strokec4  \cf13 \strokec13 FileNotFoundError\cf9 \strokec9 (\cf6 \strokec6 "Database missing."\cf9 \strokec9 )\cf4 \strokec4 \
    \cf5 \strokec5 return\cf4 \strokec4  sqlite3.connect\cf9 \strokec9 (\cf4 \strokec4 db_path\cf9 \strokec9 )\cf4 \strokec4 \
\
\cf8 \strokec8 def\cf4 \strokec4  \cf10 \strokec10 format_currency_label\cf4 \strokec4 (\cf11 \strokec11 currency_string\cf4 \strokec4 )\cf9 \strokec9 :\cf4 \strokec4 \
    \cf5 \strokec5 if\cf4 \strokec4  \cf12 \strokec12 not\cf4 \strokec4  currency_string\cf9 \strokec9 :\cf4 \strokec4  \cf5 \strokec5 return\cf4 \strokec4  \cf6 \strokec6 "Unknown"\cf4 \strokec4 \
    unique = \cf10 \strokec10 sorted\cf9 \strokec9 (\cf13 \strokec13 list\cf9 \strokec9 (\cf13 \strokec13 set\cf9 \strokec9 (\cf4 \strokec4 currency_string.split\cf9 \strokec9 (\cf6 \strokec6 ','\cf9 \strokec9 ))))\cf4 \strokec4 \
    \cf5 \strokec5 if\cf4 \strokec4  \cf10 \strokec10 len\cf9 \strokec9 (\cf4 \strokec4 unique\cf9 \strokec9 )\cf4 \strokec4  > \cf7 \strokec7 1\cf9 \strokec9 :\cf4 \strokec4  \cf5 \strokec5 return\cf4 \strokec4  \cf8 \strokec8 f\cf6 \strokec6 "Mixed (\cf9 \strokec9 \{\cf6 \strokec6 '/'\cf4 \strokec4 .join\cf9 \strokec9 (\cf4 \strokec4 unique\cf9 \strokec9 )\}\cf6 \strokec6 )"\cf4 \strokec4 \
    \cf5 \strokec5 return\cf4 \strokec4  \cf6 \strokec6 "Native EUR"\cf4 \strokec4  \cf5 \strokec5 if\cf4 \strokec4  unique\cf9 \strokec9 [\cf7 \strokec7 0\cf9 \strokec9 ]\cf4 \strokec4  == \cf6 \strokec6 'EUR'\cf4 \strokec4  \cf5 \strokec5 else\cf4 \strokec4  \cf8 \strokec8 f\cf6 \strokec6 "\cf9 \strokec9 \{\cf4 \strokec4 unique\cf9 \strokec9 [\cf7 \strokec7 0\cf9 \strokec9 ]\}\cf6 \strokec6  (Converted)"\cf4 \strokec4 \
\
\pard\pardeftab720\partightenfactor0
\cf2 \strokec2 # ==============================================================================\cf4 \strokec4 \
\cf2 \strokec2 # 3. SQL LOGIC - STRATEGY ENGINE\cf4 \strokec4 \
\cf2 \strokec2 # ==============================================================================\cf4 \strokec4 \
\
\pard\pardeftab720\partightenfactor0
\cf8 \strokec8 def\cf4 \strokec4  \cf10 \strokec10 run_strategy_engine\cf4 \strokec4 (\cf11 \strokec11 conn\cf4 \strokec4 , \cf11 \strokec11 build_id\cf4 \strokec4 , \cf11 \strokec11 usd_factor\cf4 \strokec4 )\cf9 \strokec9 :\cf4 \strokec4 \
    query = \cf8 \strokec8 f\cf6 \strokec6 """\cf4 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf6 \strokec6     WITH TargetBuild AS (\cf4 \strokec4 \
\cf6 \strokec6         SELECT ID_BUILD, FK_ID_COMPONENT, QUANTITY FROM T_SHOPPING_LIST WHERE ID_BUILD = \cf9 \strokec9 \{\cf4 \strokec4 build_id\cf9 \strokec9 \}\cf4 \strokec4 \
\cf6 \strokec6     ),\cf4 \strokec4 \
\cf6 \strokec6     Inventory_Calc AS (\cf4 \strokec4 \
\cf6 \strokec6         SELECT \cf4 \strokec4 \
\cf6 \strokec6             I.FK_ID_SUPPLIER, I.FK_ID_COMPONENT, S.NAME_SUPPLIER, S.CURRENCY_ISO, \cf4 \strokec4 \
\cf6 \strokec6             S.MINIMUM_SHIPPING_COST, S.MINIMUM_ORDER, MIN(I.PRICE) as Price_Raw_Best, \cf4 \strokec4 \
\cf6 \strokec6             CASE WHEN S.CURRENCY_ISO = 'USD' THEN MIN(I.PRICE) * \cf9 \strokec9 \{\cf4 \strokec4 usd_factor\cf9 \strokec9 \}\cf6 \strokec6  ELSE MIN(I.PRICE) END as Price_EUR_Est\cf4 \strokec4 \
\cf6 \strokec6         FROM T_INVENTORY_PRICES I JOIN T_SUPPLIERS S ON I.FK_ID_SUPPLIER = S.ID_SUPPLIER\cf4 \strokec4 \
\cf6 \strokec6         WHERE I.IS_AVAILABLE = 1 AND S.IS_ACTIVE = 1 GROUP BY I.FK_ID_SUPPLIER, I.FK_ID_COMPONENT\cf4 \strokec4 \
\cf6 \strokec6     ),\cf4 \strokec4 \
\cf6 \strokec6     Strategy_Bundle AS (\cf4 \strokec4 \
\cf6 \strokec6         SELECT \cf4 \strokec4 \
\cf6 \strokec6             'BUNDLE (Single Source)' as Strategy_Type, P.NAME_SUPPLIER as Supplier_Names,\cf4 \strokec4 \
\cf6 \strokec6             CAST(P.FK_ID_SUPPLIER as TEXT) as Supplier_IDs, P.CURRENCY_ISO as Raw_Currencies, \cf4 \strokec4 \
\cf6 \strokec6             SUM(P.Price_EUR_Est * TB.QUANTITY) as Merch_Cost_EUR,\cf4 \strokec4 \
\cf6 \strokec6             CASE WHEN P.CURRENCY_ISO = 'USD' THEN P.MINIMUM_SHIPPING_COST * \cf9 \strokec9 \{\cf4 \strokec4 usd_factor\cf9 \strokec9 \}\cf6 \strokec6  ELSE P.MINIMUM_SHIPPING_COST END as Shipping_Cost_EUR,\cf4 \strokec4 \
\cf6 \strokec6             CASE WHEN SUM(P.Price_Raw_Best * TB.QUANTITY) >= P.MINIMUM_ORDER THEN 1 ELSE 0 END as Is_Valid\cf4 \strokec4 \
\cf6 \strokec6         FROM TargetBuild TB JOIN Inventory_Calc P ON TB.FK_ID_COMPONENT = P.FK_ID_COMPONENT\cf4 \strokec4 \
\cf6 \strokec6         GROUP BY TB.ID_BUILD, P.FK_ID_SUPPLIER HAVING COUNT(TB.FK_ID_COMPONENT) = (SELECT COUNT(*) FROM TargetBuild)\cf4 \strokec4 \
\cf6 \strokec6     ),\cf4 \strokec4 \
\cf6 \strokec6     BestPrices AS (\cf4 \strokec4 \
\cf6 \strokec6         SELECT TB.FK_ID_COMPONENT, TB.QUANTITY, MIN(P.Price_EUR_Est) as Best_Price_EUR\cf4 \strokec4 \
\cf6 \strokec6         FROM TargetBuild TB JOIN Inventory_Calc P ON TB.FK_ID_COMPONENT = P.FK_ID_COMPONENT GROUP BY TB.FK_ID_COMPONENT\cf4 \strokec4 \
\cf6 \strokec6     ),\cf4 \strokec4 \
\cf6 \strokec6     Split_Logistics AS (\cf4 \strokec4 \
\cf6 \strokec6         SELECT \cf4 \strokec4 \
\cf6 \strokec6             P.FK_ID_SUPPLIER, P.NAME_SUPPLIER, P.CURRENCY_ISO, SUM(P.Price_Raw_Best * BP.QUANTITY) as Merce_Raw,\cf4 \strokec4 \
\cf6 \strokec6             SUM(BP.Best_Price_EUR * BP.QUANTITY) as Merce_EUR_Part, P.MINIMUM_SHIPPING_COST as Ship_Raw, P.MINIMUM_ORDER as Min_Order_Raw\cf4 \strokec4 \
\cf6 \strokec6         FROM BestPrices BP JOIN Inventory_Clean P ON BP.FK_ID_COMPONENT = P.FK_ID_COMPONENT AND BP.Best_Price_EUR = P.Price_EUR_Est\cf4 \strokec4 \
\cf6 \strokec6         GROUP BY P.FK_ID_SUPPLIER\cf4 \strokec4 \
\cf6 \strokec6     ),\cf4 \strokec4 \
\cf6 \strokec6     Inventory_Clean AS (SELECT * FROM Inventory_Calc), \cf4 \strokec4 \
\cf6 \strokec6     \cf4 \strokec4 \
\cf6 \strokec6     Strategy_Split AS (\cf4 \strokec4 \
\cf6 \strokec6         SELECT \cf4 \strokec4 \
\cf6 \strokec6             CASE WHEN COUNT(DISTINCT FK_ID_SUPPLIER) > 1 THEN 'SPLIT (Multi-Vendor)' ELSE 'OPTIMIZED (Mono-Vendor)' END as Strategy_Type,\cf4 \strokec4 \
\cf6 \strokec6             GROUP_CONCAT(DISTINCT NAME_SUPPLIER) as Supplier_Names, GROUP_CONCAT(DISTINCT FK_ID_SUPPLIER) as Supplier_IDs,\cf4 \strokec4 \
\cf6 \strokec6             GROUP_CONCAT(CURRENCY_ISO) as Raw_Currencies, SUM(Merce_EUR_Part) as Merch_Cost_EUR,\cf4 \strokec4 \
\cf6 \strokec6             SUM(CASE WHEN CURRENCY_ISO = 'USD' THEN Ship_Raw * \cf9 \strokec9 \{\cf4 \strokec4 usd_factor\cf9 \strokec9 \}\cf6 \strokec6  ELSE Ship_Raw END) as Shipping_Cost_EUR,\cf4 \strokec4 \
\cf6 \strokec6             MIN(CASE WHEN Merce_Raw >= Min_Order_Raw THEN 1 ELSE 0 END) as Is_Valid\cf4 \strokec4 \
\cf6 \strokec6         FROM Split_Logistics\cf4 \strokec4 \
\cf6 \strokec6     )\cf4 \strokec4 \
\cf6 \strokec6     SELECT Strategy_Type, Supplier_Names, Supplier_IDs, Raw_Currencies, Merch_Cost_EUR, Shipping_Cost_EUR,\cf4 \strokec4 \
\cf6 \strokec6     (Merch_Cost_EUR + Shipping_Cost_EUR) as Total_Cost_EUR, Is_Valid\cf4 \strokec4 \
\cf6 \strokec6     FROM (SELECT * FROM Strategy_Bundle UNION ALL SELECT * FROM Strategy_Split)\cf4 \strokec4 \
\cf6 \strokec6     ORDER BY Is_Valid DESC, Total_Cost_EUR ASC\cf4 \strokec4 \
\cf6 \strokec6     """\cf4 \strokec4 \
    \cf5 \strokec5 return\cf4 \strokec4  pd.read_sql\cf9 \strokec9 (\cf4 \strokec4 query\cf9 \strokec9 ,\cf4 \strokec4  conn\cf9 \strokec9 )\cf4 \strokec4 \
\
\pard\pardeftab720\partightenfactor0
\cf2 \strokec2 # ==============================================================================\cf4 \strokec4 \
\cf2 \strokec2 # 4. SQL LOGIC - FORENSIC DRILL DOWN\cf4 \strokec4 \
\cf2 \strokec2 # ==============================================================================\cf4 \strokec4 \
\
\pard\pardeftab720\partightenfactor0
\cf8 \strokec8 def\cf4 \strokec4  \cf10 \strokec10 run_forensic_analysis\cf4 \strokec4 (\cf11 \strokec11 conn\cf4 \strokec4 , \cf11 \strokec11 build_id\cf4 \strokec4 , \cf11 \strokec11 usd_factor\cf4 \strokec4 )\cf9 \strokec9 :\cf4 \strokec4 \
    query = \cf8 \strokec8 f\cf6 \strokec6 """\cf4 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf6 \strokec6     WITH TargetBuild AS (SELECT FK_ID_COMPONENT, QUANTITY FROM T_SHOPPING_LIST WHERE ID_BUILD = \cf9 \strokec9 \{\cf4 \strokec4 build_id\cf9 \strokec9 \}\cf6 \strokec6 ),\cf4 \strokec4 \
\cf6 \strokec6     Inventory_Calc AS (\cf4 \strokec4 \
\cf6 \strokec6         SELECT I.FK_ID_SUPPLIER, I.FK_ID_COMPONENT, S.NAME_SUPPLIER, S.CURRENCY_ISO, S.MINIMUM_SHIPPING_COST,\cf4 \strokec4 \
\cf6 \strokec6         CASE WHEN S.CURRENCY_ISO = 'USD' THEN I.PRICE * \cf9 \strokec9 \{\cf4 \strokec4 usd_factor\cf9 \strokec9 \}\cf6 \strokec6  ELSE I.PRICE END as Price_EUR\cf4 \strokec4 \
\cf6 \strokec6         FROM T_INVENTORY_PRICES I JOIN T_SUPPLIERS S ON I.FK_ID_SUPPLIER = S.ID_SUPPLIER WHERE I.IS_AVAILABLE = 1\cf4 \strokec4 \
\cf6 \strokec6     ),\cf4 \strokec4 \
\cf6 \strokec6     Rankings AS (\cf4 \strokec4 \
\cf6 \strokec6         SELECT S.NAME_SUPPLIER, RANK() OVER (ORDER BY (SUM(P.Price_EUR * TB.QUANTITY) + S.MINIMUM_SHIPPING_COST) ASC) as Position\cf4 \strokec4 \
\cf6 \strokec6         FROM TargetBuild TB JOIN Inventory_Calc P ON TB.FK_ID_COMPONENT = P.FK_ID_COMPONENT\cf4 \strokec4 \
\cf6 \strokec6         JOIN T_SUPPLIERS S ON P.FK_ID_SUPPLIER = S.ID_SUPPLIER GROUP BY S.NAME_SUPPLIER\cf4 \strokec4 \
\cf6 \strokec6         HAVING COUNT(TB.FK_ID_COMPONENT) = (SELECT COUNT(*) FROM TargetBuild)\cf4 \strokec4 \
\cf6 \strokec6     )\cf4 \strokec4 \
\cf6 \strokec6     SELECT \cf4 \strokec4 \
\cf6 \strokec6         P.FK_ID_COMPONENT as Component_ID, \cf4 \strokec4 \
\cf6 \strokec6         C.NAME_COMPONENT as Component_Name, \cf4 \strokec4 \
\cf6 \strokec6         TB.QUANTITY as Qty,\cf4 \strokec4 \
\cf6 \strokec6         \cf4 \strokec4 \
\cf6 \strokec6         -- WINNER\cf4 \strokec4 \
\cf6 \strokec6         MAX(CASE WHEN R.Position = 1 THEN P.Price_EUR END) as Winner_Price_EUR,\cf4 \strokec4 \
\cf6 \strokec6         MAX(CASE WHEN R.Position = 1 THEN R.NAME_SUPPLIER END) as Winner_Name,\cf4 \strokec4 \
\cf6 \strokec6         MAX(CASE WHEN R.Position = 1 THEN P.FK_ID_SUPPLIER END) as Winner_ID,\cf4 \strokec4 \
\cf6 \strokec6         MAX(CASE WHEN R.Position = 1 THEN P.CURRENCY_ISO END) as Winner_Orig_Currency,\cf4 \strokec4 \
\cf6 \strokec6         \cf4 \strokec4 \
\cf6 \strokec6         -- CHALLENGER (FIXED: Added ID and Currency)\cf4 \strokec4 \
\cf6 \strokec6         MAX(CASE WHEN R.Position = 2 THEN P.Price_EUR END) as Challenger_Price_EUR,\cf4 \strokec4 \
\cf6 \strokec6         MAX(CASE WHEN R.Position = 2 THEN R.NAME_SUPPLIER END) as Challenger_Name,\cf4 \strokec4 \
\cf6 \strokec6         MAX(CASE WHEN R.Position = 2 THEN P.FK_ID_SUPPLIER END) as Challenger_ID,\cf4 \strokec4 \
\cf6 \strokec6         MAX(CASE WHEN R.Position = 2 THEN P.CURRENCY_ISO END) as Challenger_Orig_Currency,\cf4 \strokec4 \
\cf6 \strokec6         \cf4 \strokec4 \
\cf6 \strokec6         -- DELTA\cf4 \strokec4 \
\cf6 \strokec6         (MAX(CASE WHEN R.Position = 1 THEN P.Price_EUR END) - MAX(CASE WHEN R.Position = 2 THEN P.Price_EUR END)) * TB.QUANTITY as Delta_Total_EUR\cf4 \strokec4 \
\cf6 \strokec6     \cf4 \strokec4 \
\cf6 \strokec6     FROM TargetBuild TB JOIN T_COMPONENTS C ON TB.FK_ID_COMPONENT = C.ID_COMPONENT\cf4 \strokec4 \
\cf6 \strokec6     JOIN Inventory_Calc P ON TB.FK_ID_COMPONENT = P.FK_ID_COMPONENT JOIN Rankings R ON P.NAME_SUPPLIER = R.NAME_SUPPLIER\cf4 \strokec4 \
\cf6 \strokec6     WHERE R.Position <= 2 GROUP BY TB.FK_ID_COMPONENT ORDER BY Delta_Total_EUR ASC\cf4 \strokec4 \
\cf6 \strokec6     """\cf4 \strokec4 \
    \cf5 \strokec5 return\cf4 \strokec4  pd.read_sql\cf9 \strokec9 (\cf4 \strokec4 query\cf9 \strokec9 ,\cf4 \strokec4  conn\cf9 \strokec9 )\cf4 \strokec4 \
\
\pard\pardeftab720\partightenfactor0
\cf2 \strokec2 # ==============================================================================\cf4 \strokec4 \
\cf2 \strokec2 # 5. EXECUTION & DOWNLOAD\cf4 \strokec4 \
\cf2 \strokec2 # ==============================================================================\cf4 \strokec4 \
\
\pard\pardeftab720\partightenfactor0
\cf10 \strokec10 print\cf9 \strokec9 (\cf6 \strokec6 "\uc0\u55357 \u56960  Starting Engine v4.4 (Full Traceability)..."\cf9 \strokec9 )\cf4 \strokec4 \
usd_factor = get_live_forex_factor\cf9 \strokec9 ()\cf4 \strokec4 \
conn = connect_db\cf9 \strokec9 (\cf4 \strokec4 DB_FILENAME\cf9 \strokec9 )\cf4 \strokec4 \
\
\pard\pardeftab720\partightenfactor0
\cf2 \strokec2 # Run Logic\cf4 \strokec4 \
df_strategy = run_strategy_engine\cf9 \strokec9 (\cf4 \strokec4 conn\cf9 \strokec9 ,\cf4 \strokec4  TARGET_BUILD_ID\cf9 \strokec9 ,\cf4 \strokec4  usd_factor\cf9 \strokec9 )\cf4 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf5 \strokec5 if\cf4 \strokec4  \cf12 \strokec12 not\cf4 \strokec4  df_strategy.empty\cf9 \strokec9 :\cf4 \strokec4 \
    df_strategy\cf9 \strokec9 [\cf6 \strokec6 'Currency_Status'\cf9 \strokec9 ]\cf4 \strokec4  = df_strategy\cf9 \strokec9 [\cf6 \strokec6 'Raw_Currencies'\cf9 \strokec9 ]\cf4 \strokec4 .apply\cf9 \strokec9 (\cf4 \strokec4 format_currency_label\cf9 \strokec9 )\cf4 \strokec4 \
    cols = \cf9 \strokec9 [\cf6 \strokec6 'Strategy_Type'\cf9 \strokec9 ,\cf4 \strokec4  \cf6 \strokec6 'Supplier_Names'\cf9 \strokec9 ,\cf4 \strokec4  \cf6 \strokec6 'Supplier_IDs'\cf9 \strokec9 ,\cf4 \strokec4  \cf6 \strokec6 'Currency_Status'\cf9 \strokec9 ,\cf4 \strokec4  \
            \cf6 \strokec6 'Merch_Cost_EUR'\cf9 \strokec9 ,\cf4 \strokec4  \cf6 \strokec6 'Shipping_Cost_EUR'\cf9 \strokec9 ,\cf4 \strokec4  \cf6 \strokec6 'Total_Cost_EUR'\cf9 \strokec9 ,\cf4 \strokec4  \cf6 \strokec6 'Is_Valid'\cf9 \strokec9 ]\cf4 \strokec4 \
    df_strategy = df_strategy\cf9 \strokec9 [\cf4 \strokec4 cols\cf9 \strokec9 ]\cf4 \strokec4 \
\
df_forensic = pd.DataFrame\cf9 \strokec9 ()\cf4 \strokec4 \
\cf5 \strokec5 if\cf4 \strokec4  \cf10 \strokec10 len\cf9 \strokec9 (\cf4 \strokec4 df_strategy\cf9 \strokec9 )\cf4 \strokec4  >= \cf7 \strokec7 2\cf9 \strokec9 :\cf4 \strokec4 \
    \cf10 \strokec10 print\cf9 \strokec9 (\cf6 \strokec6 "\uc0\u55357 \u56589  Forensic Drill-Down running..."\cf9 \strokec9 )\cf4 \strokec4 \
    df_forensic = run_forensic_analysis\cf9 \strokec9 (\cf4 \strokec4 conn\cf9 \strokec9 ,\cf4 \strokec4  TARGET_BUILD_ID\cf9 \strokec9 ,\cf4 \strokec4  usd_factor\cf9 \strokec9 )\cf4 \strokec4 \
\
\pard\pardeftab720\partightenfactor0
\cf2 \strokec2 # Save & Download\cf4 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf10 \strokec10 print\cf9 \strokec9 (\cf8 \strokec8 f\cf6 \strokec6 "\\n\uc0\u55357 \u56510  Creating Excel file: \cf9 \strokec9 \{\cf4 \strokec4 REPORT_FILENAME\cf9 \strokec9 \}\cf6 \strokec6 "\cf9 \strokec9 )\cf4 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf5 \strokec5 with\cf4 \strokec4  pd.ExcelWriter\cf9 \strokec9 (\cf4 \strokec4 REPORT_FILENAME\cf9 \strokec9 )\cf4 \strokec4  \cf5 \strokec5 as\cf4 \strokec4  writer\cf9 \strokec9 :\cf4 \strokec4 \
    df_strategy.to_excel\cf9 \strokec9 (\cf4 \strokec4 writer\cf9 \strokec9 ,\cf4 \strokec4  sheet_name=\cf6 \strokec6 'Executive_Summary'\cf9 \strokec9 ,\cf4 \strokec4  index=\cf14 \strokec14 False\cf9 \strokec9 )\cf4 \strokec4 \
    \cf5 \strokec5 if\cf4 \strokec4  \cf12 \strokec12 not\cf4 \strokec4  df_forensic.empty\cf9 \strokec9 :\cf4 \strokec4 \
        df_forensic.to_excel\cf9 \strokec9 (\cf4 \strokec4 writer\cf9 \strokec9 ,\cf4 \strokec4  sheet_name=\cf6 \strokec6 'Forensic_Drill_Down'\cf9 \strokec9 ,\cf4 \strokec4  index=\cf14 \strokec14 False\cf9 \strokec9 )\cf4 \strokec4 \
\
\pard\pardeftab720\partightenfactor0
\cf10 \strokec10 print\cf9 \strokec9 (\cf6 \strokec6 "\uc0\u9989  REPORT GENERATED. Downloading now..."\cf9 \strokec9 )\cf4 \strokec4 \
\pard\pardeftab720\partightenfactor0
\cf5 \strokec5 if\cf4 \strokec4  os.path.exists\cf9 \strokec9 (\cf4 \strokec4 REPORT_FILENAME\cf9 \strokec9 ):\cf4 \strokec4 \
    files.download\cf9 \strokec9 (\cf4 \strokec4 REPORT_FILENAME\cf9 \strokec9 )\cf4 \strokec4 \
\cf5 \strokec5 else\cf9 \strokec9 :\cf4 \strokec4 \
    \cf10 \strokec10 print\cf9 \strokec9 (\cf6 \strokec6 "\uc0\u10060  Critical Error: File write failed."\cf9 \strokec9 )\cf4 \strokec4 \
\
conn.close\cf9 \strokec9 ()\cf4 \strokec4 \
}