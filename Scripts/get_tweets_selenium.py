from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import json
import time
import os
from getpass import getpass
from datetime import datetime
import logging

class TwitterSeleniumScraper:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.logger = logging.getLogger(__name__)

    def setup_driver(self):
        """Setup and return Firefox webdriver"""
        try:
            options = Options()
            options.add_argument('--headless')  # Recommended for Airflow
            self.driver = webdriver.Firefox(options=options)
            self.wait = WebDriverWait(self.driver, 20)
            self.logger.info("Firefox driver setup successful")
            return self.driver
        except Exception as e:
            self.logger.error(f"Failed to setup Firefox driver: {e}")
            raise

    def login_to_twitter(self, username, password):
        """Login to Twitter using username and password"""
        try:
            # Go to login page
            self.driver.get("https://x.com/i/flow/login")
            time.sleep(3)
            
            # Enter username
            username_input = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[autocomplete="username"]')))
            username_input.send_keys(username)
            username_input.send_keys(Keys.RETURN)
            time.sleep(2)
            
            # Handle possible verification
            try:
                verify_input = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[data-testid="ocfEnterTextTextInput"]')))
                verify_input.send_keys(username)
                verify_input.send_keys(Keys.RETURN)
                time.sleep(2)
            except TimeoutException:
                pass
            
            # Enter password
            password_input = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[name="password"]')))
            password_input.send_keys(password)
            password_input.send_keys(Keys.RETURN)
            time.sleep(2)
            
            # Check login success
            try:
                self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a[data-testid="AppTabBar_Home_Link"]')))
                self.logger.info("Login successful!")
                return True
            except TimeoutException:
                self.logger.error("Login failed!")
                return False
                
        except Exception as e:
            self.logger.error(f"Error during login: {str(e)}")
            return False

    def load_cookies(self):
        """Load cookies from cookies.json file"""
        if not os.path.exists('cookies.json'):
            self.logger.info("No cookies.json file found")
            return False
        
        try:
            with open('cookies.json', 'r') as f:
                cookies = json.load(f)
                for cookie in cookies:
                    self.driver.add_cookie(cookie)
            return True
        except Exception as e:
            self.logger.error(f"Error loading cookies: {str(e)}")
            return False

    def get_tweet_ids_from_search(self, num_tweets=3):
        """Fetch tweet IDs from search results"""
        tweet_ids = []
        
        try:
            self.driver.get("https://x.com/explore")
            time.sleep(5)
            
            # Search for keywords
            search_box = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[data-testid="SearchBox_Search_Input"]')))
            search_box.send_keys('(Inwi OR Orange OR IAM OR Maroc Telecom) AND (telecom OR telco OR Morocco)')
            search_box.send_keys(Keys.RETURN)
            time.sleep(5)
            
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            while len(tweet_ids) < num_tweets:
                tweets = self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'article[data-testid="tweet"]')))
                
                for tweet in tweets:
                    try:
                        tweet_link = tweet.find_element(By.CSS_SELECTOR, 'a[href*="/status/"]').get_attribute('href')
                        tweet_id = self.get_tweet_id_from_url(tweet_link)
                        
                        if tweet_id and tweet_id not in tweet_ids:
                            tweet_ids.append(tweet_id)
                            self.logger.info(f"Found tweet {len(tweet_ids)}/{num_tweets}: {tweet_id}")
                            
                            if len(tweet_ids) >= num_tweets:
                                return tweet_ids
                    except NoSuchElementException:
                        continue
                
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    self.logger.info("No more tweets found in search results")
                    break
                last_height = new_height
                
        except Exception as e:
            self.logger.error(f"Error fetching tweet IDs: {str(e)}")
        
        return tweet_ids

    def get_replies_for_tweet(self, tweet_id, num_replies=3):
        """Fetch reply IDs for a specific tweet"""
        replies = []
        
        try:
            self.driver.get(f"https://x.com/anyuser/status/{tweet_id}")
            time.sleep(3)
            
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            
            while len(replies) < num_replies:
                reply_elements = self.driver.find_elements(By.CSS_SELECTOR, 'article[data-testid="tweet"]')[1:]
                
                for reply in reply_elements:
                    try:
                        reply.find_element(By.CSS_SELECTOR, '[data-testid="reply"]')
                        reply_link = reply.find_element(By.CSS_SELECTOR, 'a[href*="/status/"]').get_attribute('href')
                        reply_id = self.get_tweet_id_from_url(reply_link)
                        
                        if reply_id and reply_id not in replies:
                            replies.append(reply_id)
                            self.logger.info(f"Found reply {len(replies)}/{num_replies}: {reply_id}")
                            
                            if len(replies) >= num_replies:
                                return replies
                    except NoSuchElementException:
                        continue
                
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    self.logger.info(f"No more replies found for tweet {tweet_id}")
                    break
                last_height = new_height
                
        except Exception as e:
            self.logger.error(f"Error fetching replies for tweet {tweet_id}: {str(e)}")
        
        return replies

    @staticmethod
    def get_tweet_id_from_url(url):
        """Extract tweet ID from tweet URL"""
        try:
            return url.split('/')[-1]
        except:
            return None


    def scrape_tweets(self, execution_date=None, **context):
        """Main function to fetch tweets and their replies"""
        if execution_date is None:
            execution_date = datetime.now()
        
        date_str = execution_date.strftime('%Y-%m-%d')
        output_file = f'data/raw/tweets_and_replies_{date_str}.json'
        
        self.setup_driver()
        results = {"tweets": []}
        
        try:
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

            self.driver.get("https://x.com")
            time.sleep(2)
            
            # Modified credential handling
            username = os.getenv('TWITTER_USERNAME')
            password = os.getenv('TWITTER_PASSWORD')
            
            # If credentials not in environment variables, prompt for input
            if not username:
                username = input("Enter your Twitter username: ")
            if not password:
                password = getpass("Enter your Twitter password: ")
            
            if self.load_cookies():
                self.logger.info("Cookies loaded successfully")
                self.driver.refresh()
                time.sleep(3)
            else:
                self.logger.info("No cookies found, proceeding with login")
                if not self.login_to_twitter(username, password):
                    raise Exception("Failed to login to Twitter")
            
            tweet_ids = self.get_tweet_ids_from_search(num_tweets=5)
            
            for tweet_id in tweet_ids:
                tweet_data = {
                    "tweet_id": tweet_id,
                    "replies": self.get_replies_for_tweet(tweet_id, num_replies=10),
                    "collection_date": date_str
                }
                results["tweets"].append(tweet_data)
            
            with open(output_file, 'w') as f:
                json.dump(results, f, indent=4)
                
            self.logger.info(f"Successfully saved results to {output_file}")
            return output_file
                
        except Exception as e:
            self.logger.error(f"An error occurred: {str(e)}")
            raise
        
        finally:
            if self.driver:
                self.driver.quit()


def scrape_tweets_task(**context):
    """Wrapper function for Airflow task"""
    scraper = TwitterSeleniumScraper()
    return scraper.scrape_tweets(**context)

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        scraper = TwitterSeleniumScraper()
        output_file = scraper.scrape_tweets()
        print(f"Results saved to {output_file}")
    except Exception as e:
        print(f"Script failed: {e}")