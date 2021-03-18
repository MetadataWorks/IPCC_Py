import requests
import bs4

URL = f"https://ipcc-data.org/"

def pull_site(url):
    raw_site_page = requests.get(url)
    raw_site_page.raise_for_status()
    return raw_site_page


def scrape(raw_web_site):
    header_list = []

    soup = bs4.BeautifulSoup(raw_web_site.status_text, 'html.parser')
    aard = 'vark'
    #html_header_list = soup.select('')


def main():
    raw_site = pull_site(URL)
    scrape(raw_site)
    print(f"done")
    return


if '__main__'==__name__:
    main()
