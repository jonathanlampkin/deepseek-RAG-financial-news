import os
import json
import requests
from datetime import datetime, timedelta
import logging
from typing import List, Dict
import time
from urllib.parse import urlencode

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class NewsAPIFetcher:
    BASE_URL = "https://newsapi.org/v2/everything"
    
    def __init__(self, api_key: str = None):
        """Initialize NewsAPI client with API key."""
        self.api_key = api_key or os.getenv('API_KEY')
        if not self.api_key:
            raise ValueError("API_KEY not found in environment variables")
        
        self.output_file = os.path.join('data', 'raw_news.json')
        self.session = requests.Session()
        self.session.headers.update({'Authorization': self.api_key})

    def fetch_financial_news(
        self,
        days_back: int = 30,
        sources: List[str] = None,
        save: bool = True
    ) -> List[Dict]:
        """
        Fetch financial news articles from the past specified days.
        
        Args:
            days_back: Number of days to look back
            sources: List of news sources to query
            save: Whether to save the results to file
            
        Returns:
            List of news articles
        """
        if sources is None:
            sources = [
                'bloomberg',
                'business-insider',
                'financial-times',
                'fortune',
                'reuters',
                'the-wall-street-journal'
            ]

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        params = {
            'q': 'finance OR stock OR market OR trading OR investment',
            'sources': ','.join(sources),
            'from': start_date.strftime('%Y-%m-%d'),
            'to': end_date.strftime('%Y-%m-%d'),
            'language': 'en',
            'sortBy': 'publishedAt',
            'pageSize': 100,  # Maximum allowed by API
            'page': 1
        }
        
        all_articles = []
        
        try:
            while True:
                logger.info(f"Fetching page {params['page']} of articles...")
                
                url = f"{self.BASE_URL}?{urlencode(params)}"
                response = self.session.get(url)
                response.raise_for_status()  # Raise exception for bad status codes
                
                data = response.json()
                
                if data['status'] != 'ok':
                    raise Exception(f"API Error: {data.get('message', 'Unknown error')}")
                
                articles = data.get('articles', [])
                if not articles:
                    break
                    
                all_articles.extend(articles)
                
                # Check if we've reached the total results
                if len(all_articles) >= data.get('totalResults', 0):
                    break
                
                # NewsAPI free tier only allows up to 100 results
                if len(all_articles) >= 100:
                    break
                    
                params['page'] += 1
                time.sleep(1)  # Rate limiting

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error fetching news: {str(e)}")
            raise

        logger.info(f"Successfully fetched {len(all_articles)} articles")

        if save:
            self._save_articles(all_articles)

        return all_articles

    def _save_articles(self, articles: List[Dict]) -> None:
        """Save articles to JSON file."""
        os.makedirs('data', exist_ok=True)
        
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(articles, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(articles)} articles to {self.output_file}")
        except Exception as e:
            logger.error(f"Error saving articles: {str(e)}")
            raise

def main():
    """Main function to demonstrate usage."""
    fetcher = NewsAPIFetcher()
    articles = fetcher.fetch_financial_news(days_back=7)
    print(f"Fetched {len(articles)} articles")

if __name__ == "__main__":
    main()
