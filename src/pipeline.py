import getopt
import sys
from time import sleep

import data_scraper as ds
import data_cleaner as dc
import data_ingester as di
import common_utils as cu


class Pipeline():
    def __init__(self):
        self.logger = cu.create_log()


class LinkedinPipeline(Pipeline):
    def execute_flow(self):
        # Scrape
        scraper = ds.LinkedinScraper()
        raw_records = scraper.scrape_data()
        # Clean
        cleaner = dc.LinkedinCleaner()
        for author in raw_records.keys():
            clean_records = cleaner.get_clean_records(raw_record=raw_records[author], author=author)
            self.logger.info(clean_records)
            # Ingest
            ingester = di.LinkedinIngester()
            ingester.ingest_data(clean_records=clean_records)
            sleep(0.06)


def main(run_type: str):
    if run_type == cu.INGEST_RT:
        LinkedinPipeline().execute_flow()
    else:
        pass


if __name__ == "__main__":
    main(run_type=cu.INGEST_RT)