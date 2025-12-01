from ckanext.matomo.matomo_api import MatomoAPI

if __name__ == "__main__":
    instance = MatomoAPI(matomo_url='http://localhost:8081', id_site=1, token_auth='db335ed924a133a7dd52018005333598')

    stats = instance.dataset_page_statistics(period='day', date='2025-07-23')
    print(stats)
