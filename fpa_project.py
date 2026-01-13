import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus

#--| DATABASE CONNECTION & DATA LOADING |--

password = quote_plus("Sahil@10")

engine = create_engine(
    f"mysql+pymysql://root:{password}@localhost:3306/fpna_project"
)

query = "SELECT * FROM fmcg_data"
df = pd.read_sql(query, engine)

print(df)

#--| DATA UNDERSTANDING |--

print("Sample data:")
print(df.head())

print("\nData structure:")
print(df.info())

print("\nMissing values:")
print(df.isna().sum())

#--| STANDARDIZING COLUMN NAMES |--

df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
)

print("std column names")
print(df)

#--| DATE CLEANING |--

df['month'] = pd.to_datetime(df['month'], errors='coerce')
df = df.sort_values(by='month')

print("Date fixed")
print(df)

#--| DATA CLEANING |--

numeric_cols = [
    'gross_sales', 'discounts', 'net_sales', 'cogs',
    'marketing_expense', 'admin_expense',
    'depreciation', 'ebitda',
    'operating_cash_flow', 'capex', 'financing_cash_flow',
    'cash', 'accounts_receivable', 'inventory',
    'accounts_payable', 'debt'
]

for col in numeric_cols:
    df[col] = (
        df[col]
        .astype(str)
        .str.replace(',', '')
        .str.replace('₹', '')
        .str.strip()
    )
    df[col] = pd.to_numeric(df[col], errors='coerce')

df[numeric_cols] = df[numeric_cols].fillna(0)

print("cleaned columns:")
print(df)

#--| FINANCIAL KPIs |--

# | GROSS PROFIT & MARGIN |
df['gross_profit'] = df['net_sales'] - df['cogs']
df['gross_margin_%'] = (df['gross_profit'] / df['net_sales']) * 100

print("gross profit & margin:")
print(df)

# | OPERATING EXPENSES |
df['operating_expenses'] = (
    df['marketing_expense'] + df['admin_expense']
)
print("operating expenses:")
print(df)

# | EBIT |
df['ebit'] = df['ebitda'] - df['depreciation']
print("ebit")
print(df)

# | EBITDA MARGIN |
df['ebitda_margin_%'] = (df['ebitda'] / df['net_sales']) * 100
print("ebitda margin %:")
print(df)

#--| GROWTH METRICS |--

df['net_sales_mom_growth_%'] = df['net_sales'].pct_change() * 100
df['ebitda_mom_growth_%'] = df['ebitda'].pct_change() * 100
print("ebitda mom growth %:")
print(df)

#--| CASH FLOW & BALANCE SHEET KPIs |--

df['free_cash_flow'] = df['operating_cash_flow'] - df['capex']
print("free cash flow:")
print(df)

df['capex_to_sales_%'] = (df['capex'] / df['net_sales']) * 100
print("capex to sales %:")
print(df)

df['working_capital'] = (
    df['accounts_receivable'] + df['inventory'] - df['accounts_payable']
)
print("working capital:")
print(df)

df['net_debt'] = df['debt'] - df['cash']
print("net debt:")
print(df)


#--| SALES BUDGET |--
# By assuming last year actual & 5% growth

df = df.sort_values('month')
df['budget_net_sales'] = (
    df.groupby(['product', 'region'])['net_sales']
      .shift(12) * 1.05
)
df['budget_net_sales'] = df['budget_net_sales'].fillna(0)
print("budget net sales:")
print(df)

#--| VARIANCE ANALYSIS |--

df['net_sales_variance'] = df['net_sales'] - df['budget_net_sales']

df['net_sales_variance_pct'] = (
    df['net_sales_variance'] / df['budget_net_sales']
)

df.replace([float('inf'), -float('inf')], 0, inplace=True)
df.fillna(0, inplace=True)

print("net sales variance:")
print(df)


df.to_csv("FPNA_FMCG_Final_Clean_Data.csv", index=False)
