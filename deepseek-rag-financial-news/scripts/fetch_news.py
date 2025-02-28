import os
import json
import requests
import logging
from typing import List, Dict, Optional
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import random
from urllib.parse import urljoin
import concurrent.futures
import cloudscraper  # For bypassing Cloudflare protection
from fake_useragent import UserAgent  # For better user agent rotation

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FinancialNewsScraper:
    """Scraper for collecting financial news from multiple free sources."""
    
    # News sources configuration with more reliable endpoints
    SOURCES = {
        'yahoo_finance': {
            'name': 'Yahoo Finance',
            'url': 'https://finance.yahoo.com/news/',
            'article_selector': 'div.Ov\(h\).Pend\(44px\).Pstart\(25px\)',
            'title_selector': 'h3',
            'link_selector': 'a',
            'summary_selector': 'p',
            'date_selector': 'span.C\(#959595\)'
        },
        'cnbc': {
            'name': 'CNBC',
            'url': 'https://www.cnbc.com/world/?region=world',
            'article_selector': '.Card-standardBreakerCard, .Card-card',
            'title_selector': '.Card-title',
            'link_selector': 'a',
            'summary_selector': '.Card-description',
            'date_selector': 'time'
        },
        'seeking_alpha': {
            'name': 'Seeking Alpha',
            'url': 'https://seekingalpha.com/market-news',
            'article_selector': 'div.media-preview-content',
            'title_selector': 'a.media-link',
            'link_selector': 'a.media-link',
            'summary_selector': 'p.media-preview-summary',
            'date_selector': 'span.media-date'
        },
        'finviz': {
            'name': 'Finviz',
            'url': 'https://finviz.com/news.ashx',
            'article_selector': 'tr.nn',
            'title_selector': 'a.nn-tab-link',
            'link_selector': 'a.nn-tab-link',
            'summary_selector': None,
            'date_selector': 'td.nn-date'
        },
        'market_watch_latest': {
            'name': 'MarketWatch Latest',
            'url': 'https://www.marketwatch.com/latest-news?mod=top_nav',
            'article_selector': 'div.article__content',
            'title_selector': 'h3.article__headline',
            'link_selector': 'a',
            'summary_selector': 'p.article__summary',
            'date_selector': '.article__timestamp'
        },
        'bloomberg': {
            'name': 'Bloomberg',
            'url': 'https://www.bloomberg.com/markets',
            'article_selector': 'article.story-list-story',
            'title_selector': 'h3.story-list-story__headline',
            'link_selector': 'a.story-list-story__info__headline-link',
            'summary_selector': 'p.story-list-story__summary',
            'date_selector': 'time.story-list-story__time'
        },
        'investing_analysis': {
            'name': 'Investing.com Analysis',
            'url': 'https://www.investing.com/analysis/most-popular-analysis',
            'article_selector': 'article.articleItem',
            'title_selector': '.title',
            'link_selector': 'a.title',
            'summary_selector': 'p',
            'date_selector': '.date'
        },
        'zacks': {
            'name': 'Zacks',
            'url': 'https://www.zacks.com/stock-market-today',
            'article_selector': '.commentary_module_row',
            'title_selector': 'a',
            'link_selector': 'a',
            'summary_selector': 'p',
            'date_selector': '.date_time'
        }
    }
    
    def __init__(self):
        """Initialize the scraper with advanced request handling."""
        # Use cloudscraper to bypass Cloudflare protection
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            }
        )
        
        # Use fake_useragent for better user agent rotation
        try:
            self.user_agent = UserAgent()
        except:
            # Fallback user agents if fake_useragent fails
            self.user_agent = None
            self.USER_AGENTS = [
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36',
                'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
                'Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1'
            ]
        
        self.output_file = os.path.join('data', 'raw_news.json')
        os.makedirs('data', exist_ok=True)
        
        # Add delay between requests to avoid rate limiting
        self.min_delay = 1
        self.max_delay = 3
        
    def _get_random_user_agent(self) -> str:
        """Return a random user agent."""
        if self.user_agent:
            return self.user_agent.random
        else:
            return random.choice(self.USER_AGENTS)
    
    def _make_request(self, url: str) -> Optional[str]:
        """Make an HTTP request with advanced error handling and retries."""
        headers = {
            'User-Agent': self._get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'TE': 'Trailers',
        }
        
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                # Add random delay between requests
                time.sleep(random.uniform(self.min_delay, self.max_delay))
                
                # Try with cloudscraper first
                response = self.scraper.get(url, headers=headers, timeout=15)
                response.raise_for_status()
                return response.text
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt+1}/{max_retries}): {str(e)}")
                
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    sleep_time = (2 ** attempt) + random.uniform(0, 1)
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                    return None
    
    def _parse_date(self, date_text: str) -> str:
        """Parse various date formats into ISO format."""
        try:
            date_text = date_text.lower().strip()
            
            # Handle relative dates
            if 'ago' in date_text:
                if 'minute' in date_text or 'min' in date_text:
                    minutes = int(''.join(filter(str.isdigit, date_text)))
                    date = datetime.now() - timedelta(minutes=minutes)
                elif 'hour' in date_text or 'hr' in date_text:
                    hours = int(''.join(filter(str.isdigit, date_text)))
                    date = datetime.now() - timedelta(hours=hours)
                elif 'day' in date_text:
                    days = int(''.join(filter(str.isdigit, date_text)))
                    date = datetime.now() - timedelta(days=days)
                elif 'week' in date_text:
                    weeks = int(''.join(filter(str.isdigit, date_text)))
                    date = datetime.now() - timedelta(weeks=weeks)
                elif 'month' in date_text:
                    # Approximate a month as 30 days
                    months = int(''.join(filter(str.isdigit, date_text)))
                    date = datetime.now() - timedelta(days=30*months)
                else:
                    date = datetime.now()
                return date.isoformat()
            
            # Handle "today" and "yesterday"
            if 'today' in date_text:
                return datetime.now().isoformat()
            if 'yesterday' in date_text:
                return (datetime.now() - timedelta(days=1)).isoformat()
            
            # Try common date formats
            for fmt in [
                '%Y-%m-%d', '%b %d, %Y', '%d %b %Y', '%B %d, %Y', '%d %B %Y', 
                '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d', '%m-%d-%Y', '%d-%m-%Y',
                '%b %d', '%d %b', '%B %d', '%d %B'  # Formats without year
            ]:
                try:
                    # For formats without year, add current year
                    parsed_date = datetime.strptime(date_text.strip(), fmt)
                    if parsed_date.year == 1900:
                        parsed_date = parsed_date.replace(year=datetime.now().year)
                    return parsed_date.isoformat()
                except ValueError:
                    continue
            
            # Default to current date if parsing fails
            return datetime.now().isoformat()
        except Exception as e:
            logger.warning(f"Date parsing error: {str(e)}")
            return datetime.now().isoformat()
    
    def _scrape_source(self, source_id: str, source_config: Dict) -> List[Dict]:
        """Scrape a single news source."""
        logger.info(f"Scraping news from {source_config['name']}...")
        articles = []
        
        html = self._make_request(source_config['url'])
        if not html:
            logger.warning(f"No HTML content retrieved from {source_config['name']}")
            return articles
        
        soup = BeautifulSoup(html, 'html.parser')
        article_elements = soup.select(source_config['article_selector'])
        
        logger.info(f"Found {len(article_elements)} potential article elements on {source_config['name']}")
        
        for article in article_elements:
            try:
                # Extract title
                title_element = article.select_one(source_config['title_selector'])
                if not title_element:
                    continue
                title = title_element.get_text().strip()
                
                # Extract link
                link_element = article.select_one(source_config['link_selector'])
                if not link_element or not link_element.has_attr('href'):
                    continue
                link = link_element['href']
                
                # Make sure link is absolute
                if not link.startswith(('http://', 'https://')):
                    link = urljoin(source_config['url'], link)
                
                # Extract summary if available
                summary = ""
                if source_config['summary_selector']:
                    summary_element = article.select_one(source_config['summary_selector'])
                    if summary_element:
                        summary = summary_element.get_text().strip()
                
                # Extract date if available
                published_at = datetime.now().isoformat()
                if source_config['date_selector']:
                    date_element = article.select_one(source_config['date_selector'])
                    if date_element:
                        date_text = date_element.get_text().strip()
                        published_at = self._parse_date(date_text)
                
                articles.append({
                    'source': {
                        'id': source_id,
                        'name': source_config['name']
                    },
                    'title': title,
                    'description': summary,
                    'url': link,
                    'publishedAt': published_at,
                    'content': None  # Will be filled if fetch_article_content is called
                })
            except Exception as e:
                logger.warning(f"Error parsing article from {source_config['name']}: {str(e)}")
        
        logger.info(f"Successfully extracted {len(articles)} articles from {source_config['name']}")
        return articles
    
    def fetch_article_content(self, article: Dict) -> Dict:
        """Fetch and parse the full content of an article."""
        if not article.get('url'):
            return article
        
        try:
            html = self._make_request(article['url'])
            if not html:
                return article
            
            soup = BeautifulSoup(html, 'html.parser')
            
            # Remove script, style, nav, header, footer elements
            for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
                element.decompose()
            
            # Get text from article body - try multiple common selectors
            article_body = None
            
            # Try common article body selectors
            for selector in [
                'article', '.article-body', '.article-content', '.story-body',
                '.story-content', '.post-content', '.entry-content', 'main',
                '#article-body', '#content-body', '.content-article', '.article__body',
                '[itemprop="articleBody"]', '.news-content', '.article-text'
            ]:
                article_body = soup.select_one(selector)
                if article_body:
                    break
            
            if article_body:
                # Get text and clean it up
                content = article_body.get_text(separator='\n').strip()
                # Remove excessive newlines and whitespace
                content = '\n'.join(line.strip() for line in content.split('\n') if line.strip())
                article['content'] = content
            else:
                # Fallback: get all paragraph text if no article body found
                paragraphs = soup.find_all('p')
                if paragraphs:
                    content = '\n'.join(p.get_text().strip() for p in paragraphs if p.get_text().strip())
                    article['content'] = content
        except Exception as e:
            logger.warning(f"Error fetching article content for {article['url']}: {str(e)}")
        
        return article
    
    def fetch_news(self, sources: List[str] = None, fetch_content: bool = False, save: bool = True) -> List[Dict]:
        """
        Fetch financial news from multiple sources.
        
        Args:
            sources: List of source IDs to scrape (defaults to all)
            fetch_content: Whether to fetch full article content
            save: Whether to save results to file
            
        Returns:
            List of news articles
        """
        if sources is None:
            sources = list(self.SOURCES.keys())
        else:
            # Validate sources
            sources = [s for s in sources if s in self.SOURCES]
        
        all_articles = []
        
        # Use ThreadPoolExecutor to scrape sources in parallel, but with fewer workers to avoid overloading
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(3, len(sources))) as executor:
            future_to_source = {
                executor.submit(self._scrape_source, source_id, self.SOURCES[source_id]): source_id
                for source_id in sources
            }
            
            for future in concurrent.futures.as_completed(future_to_source):
                source_id = future_to_source[future]
                try:
                    articles = future.result()
                    all_articles.extend(articles)
                except Exception as e:
                    logger.error(f"Error scraping {source_id}: {str(e)}")
        
        # Fetch full content if requested
        if fetch_content and all_articles:
            logger.info(f"Fetching full article content for {len(all_articles)} articles...")
            
            # Use a smaller number of workers for content fetching to avoid being blocked
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                all_articles = list(executor.map(self.fetch_article_content, all_articles))
        
        # Sort by published date (newest first)
        all_articles.sort(key=lambda x: x.get('publishedAt', ''), reverse=True)
        
        if save and all_articles:
            self._save_articles(all_articles)
        
        return all_articles
    
    def _save_articles(self, articles: List[Dict]) -> None:
        """Save articles to JSON file."""
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(articles, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(articles)} articles to {self.output_file}")
        except Exception as e:
            logger.error(f"Error saving articles: {str(e)}")
            raise

def main():
    """Main function to demonstrate usage."""
    scraper = FinancialNewsScraper()
    
    # Fetch news from all sources
    articles = scraper.fetch_news(fetch_content=True)
    
    print(f"Fetched {len(articles)} articles from {len(scraper.SOURCES)} sources")
    
    # Print sample of articles
    if articles:
        print("\nSample articles:")
        for i, article in enumerate(articles[:3]):
            print(f"\n{i+1}. {article['title']} ({article['source']['name']})")
            print(f"   URL: {article['url']}")
            print(f"   Date: {article['publishedAt']}")
            if article.get('description'):
                print(f"   Summary: {article['description'][:100]}...")

if __name__ == "__main__":
    main() 