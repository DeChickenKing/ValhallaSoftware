import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from playwright.sync_api import sync_playwright
import logging

# Configure logging
logging.basicConfig(filename='product_processing_log.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def format_phone_number(phone):
    """ Ensure the phone number is stored as text in Excel by prefixing with an apostrophe. """
    return f"'{phone}"

def fetch_product_details(url):
    """ Fetches product reference number and euro price from the given URL using Playwright. """
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url)

            # Selectors based on your specific HTML structure
            reference_selector = "div.reference span[itemprop='sku']"
            price_selector = "div[itemprop='offers'] span.black-text strong"

            # Wait for elements to ensure they are loaded
            page.wait_for_selector(reference_selector, timeout=5000)  # Wait up to 5000 ms or 5 seconds
            page.wait_for_selector(price_selector, timeout=5000)

            # Extracting text
            reference = page.query_selector(reference_selector).inner_text().strip() if page.query_selector(reference_selector) else "Reference not found"
            price = page.query_selector(price_selector).inner_text().strip() if page.query_selector(price_selector) else "Price not found"

            if reference == "Reference not found" or price == "Price not found":
                logging.warning(f"Failed to fetch details for URL {url}: Reference or price not found.")
            else:
                logging.info(f"Successfully fetched details for URL {url}: Reference - {reference}, Price - {price}")

            browser.close()
            return reference, price
    except Exception as e:
        logging.error(f"Exception occurred while fetching product details from {url}: {str(e)}")
        return None, None




def process_csv(file_path):
    data = pd.read_csv(file_path, dtype={'電話號碼': str})

    products = []
    bookkeeping_data = []
    incorrect_entries = []  # This will ensure that handling matches your old script
    for index, row in data.iterrows():
        items = row['商品訊息'].split('\n')
        for item in items:
            if "Product Name / 商品名稱" in item:
                try:
                    name = item.split("Product Name / 商品名稱: ")[1].split(", Product Link")[0].strip()
                    link = item.split("Product Link / 商品網址: ")[1].split(", Product Quantity")[0].strip()
                    quantity = int(item.split("Product Quantity / 所需數量: ")[1].strip())

                        # Check and log before adding to incorrect entries
                    if not link.startswith('http'):
                        logging.error(f"Invalid link format at index {index}: {link}")
                        raise ValueError("Link does not start with http")

                    if not link or not name or quantity <= 0:
                        logging.error(f"Missing or invalid data at index {index}: Name - {name}, Link - {link}, Quantity - {quantity}")
                        raise ValueError("Invalid product data")

                    if 'corvusbelli.com' in link:
                        ref_number, euro_price = fetch_product_details(link)
                        if not ref_number or not euro_price:
                            logging.error(f"Failed to fetch product details at index {index}: Reference - {ref_number}, Price - {euro_price}")
                            raise ValueError("Failed to fetch reference number or price")
                        products.append([ref_number, name, link, euro_price, quantity])
                    else:
                        products.append([name, quantity, link])

                    bookkeeping_data.append([
                        name,
                        quantity,
                        row['訂貨者名稱'],
                        format_phone_number(row['電話號碼']),
                        row['Submission Date'],
                        link
                    ])

                except Exception as e:
                    # Directly log and append to incorrect entries here
                    logging.error(f"Error at index {index} for customer {row['訂貨者名稱']}: {str(e)}")
                    incorrect_entries.append([
                        row['訂貨者名稱'],
                        format_phone_number(row['電話號碼']),
                        row['Submission Date'],
                        item  # Capture the entire item string for review
                    ])



    # DataFrame creation for Corvus Belli with extra columns
    corvus_belli_df = pd.DataFrame([p for p in products if 'corvusbelli.com' in p[2]], columns=['Reference Number', 'Product Name', 'Link', 'Euro Price', 'Quantity'])
    games_workshop_df = pd.DataFrame([p for p in products if 'warhammer' in p[2]], columns=['Product Name', 'Quantity', 'Link'])

    df_bookkeeping = pd.DataFrame(bookkeeping_data, columns=['Product Name', 'Quantity', 'Customer Name', 'Phone Number', 'Date', 'Link'])
    df_incorrect = pd.DataFrame(incorrect_entries, columns=['Customer Name', 'Phone Number', 'Date', 'Invalid Entry'])

    # File exportation
    today = datetime.now().strftime('%Y%m%d')
    directories = ['CorvusBelli_Supplier', 'GamesWorkshop_Supplier', 'GW_BookKeeping', 'Infinity_BookKeeping', 'retards']
    for directory in directories:
        ensure_dir(directory)

    corvus_belli_df.to_excel(f'CorvusBelli_Supplier/{today}_corvus_belli.xlsx', index=False)
    games_workshop_df.to_excel(f'GamesWorkshop_Supplier/{today}_games_workshop.xlsx', index=False)
    df_bookkeeping[df_bookkeeping['Link'].str.contains("corvusbelli")].sort_values(by='Date').to_excel(f'Infinity_BookKeeping/{today}_infinity_bookkeeping.xlsx', index=False)
    df_bookkeeping[df_bookkeeping['Link'].str.contains("warhammer")].sort_values(by='Date').to_excel(f'GW_BookKeeping/{today}_gw_bookkeeping.xlsx', index=False)
    if not df_incorrect.empty:
        df_incorrect.to_excel(f'retards/{today}_incorrect_entries.xlsx', index=False)

    print("Files generated and saved in respective directories.")

def parse_product_item(item):
    """ Parses product details from an item string. """
    name = item.split("Product Name / 商品名稱: ")[1].split(", Product Link")[0].strip()
    link = item.split("Product Link / 商品網址: ")[1].split(", Product Quantity")[0].strip()
    quantity = int(item.split("Product Quantity / 所需數量: ")[1].strip())
    return name, link, quantity

# Main execution
if __name__ == "__main__":
    directory = os.getcwd()
    csv_files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    for file_name in csv_files:
        process_csv(os.path.join(directory, file_name))
