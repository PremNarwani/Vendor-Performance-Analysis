import sqlite3
import pandas as pd
import logging
from ingestion_db import ingest_db
logging.basicConfig(
    filename = "logs/get_vendor_summary.log",
    level = logging.DEBUG,
    format = "%(asctime)s - %(levelname)s - %(message)s",
    filemode = 'a'
)

def create_vendor_summary(conn):

vendor_sales_summary = pd.read_sql_query("""WITH FreightSummary AS (
   SELECT 
        VendorNumber,
        SUM(Freight) AS FreightCost
   FROM vendor_invoice 
   GROUP BY VendorNumber
),
PurchaseSummary AS (
   SELECT
      p.VendorNumber,
      p.VendorName,
      p.Brand,
      p.Description,
      MAX(p.PurchasePrice) AS PurchasePrice,
      MAX(pp.Volume) AS Volume,
      MAX(pp.Price) AS ActualPrice,
      SUM(p.Quantity) AS TotalPurchaseQuantity,
      SUM(p.Dollars) AS TotalPurchaseDollars
   FROM purchases p
   JOIN purchase_prices pp
     ON p.Brand = pp.Brand
   GROUP BY p.VendorNumber, p.VendorName, p.Brand , p.Description
   HAVING MAX(p.PurchasePrice) > 0
),
SalesSummary AS (
  SELECT 
      VendorNo,
      Brand,
      MAX(SalesPrice) AS SalesPrice,
      SUM(SalesQuantity) AS TotalSalesQuantity,
      SUM(SalesDollars) AS TotalSalesDollars,
      SUM(ExciseTax) AS TotalExciseTax 
  FROM sales
  GROUP BY VendorNo, Brand
)
SELECT   
      ps.VendorNumber,
      ps.VendorName,
      ps.Brand,
      ps.Description,
      ps.PurchasePrice,
      ps.Volume,
      ps.ActualPrice,
      ps.TotalPurchaseQuantity,
      ps.TotalPurchaseDollars,
      ss.SalesPrice, 
      ss.TotalSalesQuantity,
      ss.TotalSalesDollars,
      ss.TotalExciseTax,
      fs.FreightCost
FROM PurchaseSummary ps
LEFT JOIN SalesSummary ss
    ON  ps.VendorNumber = ss.VendorNo
    AND ps.Brand = ss.Brand
LEFT JOIN FreightSummary fs
     ON ps.VendorNumber = fs.VendorNumber
ORDER BY ps.TotalPurchaseDollars DESC;
""",conn)

return vendor_sales_summary


def clean_data(df):
    vendor_sales_summary['Volume'] = vendor_sales_summary['Volume'].astype('float64')
    vendor_sales_summary['VendorName'] = vendor_sales_summary['VendorName'].str.strip()
    vendor_sales_summary.fillna(0,inplace = True)
    vendor_sales_summary['GrossProfit'] = vendor_sales_summary['TotalSalesDollars'] -vendor_sales_summary['TotalPurchaseDollars']
    vendor_sales_summary['ProfitMargin'] = (vendor_sales_summary['GrossProfit']/vendor_sales_summary['TotalSalesDollars']) * 100
    vendor_sales_summary['StockTurnover'] = vendor_sales_summary['TotalSalesQuantity']/vendor_sales_summary['TotalPurchaseQuantity']
    vendor_sales_summary['SalestoPurchaseRatio'] = vendor_sales_summary['TotalSalesDollars'] / vendor_sales_summary['TotalPurchaseDollars']

return df

if __name__ == '__main__':

    conn = sqlite3.connect('inventory.db')

    logging.info("Creating Vendor Summary Table......")
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head())

    logging.info("Cleaning Data.....")
    clean_df =  clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info("Ingesting Data")
    ingest_db(clean_df,'vendor_sales_summary',conn)
    logging_info('Completed')