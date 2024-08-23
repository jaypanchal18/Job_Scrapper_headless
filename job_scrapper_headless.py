import schedule
import time
import random
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Function to initialize WebDriver
def initialize_webdriver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Uncomment if you want to run Chrome in headless mode
    chrome_options.add_argument("--disable-gpu")  # Disable GPU acceleration
    chrome_options.add_argument("--no-sandbox")  # Disable sandboxing (often needed in headless mode)
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36")
    chrome_driver_path = 'C:/webdrivers/chromedriver.exe'
    return webdriver.Chrome(service=Service(chrome_driver_path), options=chrome_options)

# MongoDB setup
mongo_client = MongoClient("mongodb://localhost:27017/")
db = mongo_client["invite"]

# Slack bot token and client setup
slack_bot_token = "xoxb-7599666633990-7615567628180-vHIeiVcWSyu9XJwUsmSWh4BL"  # Update with your actual token
slack_client = WebClient(token=slack_bot_token)

# Mapping of categories to Slack channel names
category_to_channel_name = {
    "node": "upwork_feed_node"
}

def get_channel_ids_by_names():
    """Retrieve channel IDs based on desired channel names."""
    try:
        response = slack_client.conversations_list(types="public_channel,private_channel")
        channels = response["channels"]
        channel_ids = {
            channel["name"]: channel["id"]
            for channel in channels
            if channel["name"] in category_to_channel_name.values()
        }
        return channel_ids
    except SlackApiError as e:
        print(f"Error fetching channels: {e.response['error']}")
        return {}

def send_to_slack(job, channel_id):
    """Send a job posting to a Slack channel using the bot token."""
    message = (
        f"*New job posted:*\n\n"
        f"*Title:* {job['title']}\n\n"
        f"*Description:* {job['description']}\n\n"
        f"*Skills:* {job['skills']}\n\n"
        f"*Job Link:* <{job['link']}>"
    )
    try:
        response = slack_client.chat_postMessage(
            channel=channel_id,
            text=message
        )
        assert response["ok"]
        print(f"Message sent to Slack channel {channel_id}.")
    except SlackApiError as e:
        print(f"Error sending message to Slack: {e.response['error']}")

def wait_random_time():
    """Wait for a random time to mimic human behavior."""
    time.sleep(random.uniform(2, 5))

def scrape_and_send_jobs():
    """Scrape job postings and send them to Slack channels."""
    channel_ids = get_channel_ids_by_names()

    for category, channel_name in category_to_channel_name.items():
        print(f"Processing category: {category}")

        # Initialize WebDriver for each category
        driver = initialize_webdriver()
        
        # Create or select the MongoDB collection for the current category
        collection = db[category]
        
        # Ensure the "link" field is unique by creating an index
        collection.create_index("link", unique=True)
        
        new_jobs = []  # List to keep track of jobs to be sent to Slack
        
        try:
            # Navigate to the Upwork job search page for the current category
            url = f"https://www.upwork.com/nx/search/jobs/?q={category}"
            driver.get(url)

            # Wait for the page to load and elements to be present
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, "card-list-container"))
            )

            # Mimic human behavior
            wait_random_time()

            # Scrape data
            titles = driver.find_elements(By.CLASS_NAME, "job-tile-header")
            descriptions = driver.find_elements(By.CSS_SELECTOR, ".text-body-sm")
            skills_elements = driver.find_elements(By.CSS_SELECTOR, ".air3-token-container")
            job_links = driver.find_elements(By.CSS_SELECTOR, ".job-tile-header a")

            # Process and structure the data
            for i in range(len(titles)):
                # Combine the skills into a comma-separated string
                skills = skills_elements[i].text.strip().replace("\n", ", ") if i < len(skills_elements) else ""
                
                job_entry = {
                    "title": titles[i].text.strip() if i < len(titles) else "",
                    "description": descriptions[i].text.strip() if i < len(descriptions) else "",
                    "skills": skills,
                    "link": job_links[i].get_attribute("href") if i < len(job_links) else ""
                }

                try:
                    collection.insert_one(job_entry)
                    print(f"Inserted job: {job_entry['title']} into MongoDB.")
                    new_jobs.append(job_entry)  # Add to new_jobs only if successfully inserted
                except DuplicateKeyError:
                    print(f"Duplicate job found and skipped: {job_entry['title']}")

        except Exception as e:
            print(f"An error occurred for category {category}: {e}")

        finally:
            # Close the browser session
            driver.quit()
            # Optional: Wait before moving to the next category
            wait_random_time()

        # After scraping, send only the new jobs to the corresponding Slack channel
        channel_id = channel_ids.get(channel_name.strip())
        for job in new_jobs:
            if channel_id:
                send_to_slack(job, channel_id)
            else:
                print(f"Channel {channel_name} not found or bot is not a member.")

    print("Data successfully processed and sent to Slack channels.")

# Run the function immediately upon starting the script
scrape_and_send_jobs()

# Schedule the job to run every 5 minutes
schedule.every(5).minutes.do(scrape_and_send_jobs)

while True:
    schedule.run_pending()
    time.sleep(1)
