import scrapy
import os
import json
import jmespath
import pprint
from tqdm import tqdm

class BusinessSpider(scrapy.Spider):
    name = "business_spider" # unique name
    custom_settings = {
		"LOG_FILE": "business_spider.log",
        "LOG_LEVEL": "WARNING",
        "DEPTH_LIMIT" : 2,
        "FEEDS" : {
            "output/company_records.jsonl": {"format": "jsonl"},
        }
    }
	
    def start_requests(self):
        '''
            Begin Spider Request
        '''
        url = 'https://firststop.sos.nd.gov/api/Records/businesssearch'
        payload = {
            "SEARCH_VALUE": "X", 
            "STARTS_WITH_YN": "true", 
            "ACTIVE_ONLY_YN": True
        }
        headers = {
            "Content-Type": "application/json",
            "Accept" : "application/json"
        }
        yield scrapy.Request(
            url=url,
            method="POST",  
            body=json.dumps(payload),
            headers=headers,
            callback=self.parse
        )
	
    def parse(self, response):
        '''
            Parse data from initial web app request. Response
            should be Python dictionary converted from JSON.
        '''
		# Get all businesses
        all_businesses_json = json.loads(response.text)

        # Get dictionary under 'rows'.
        # For the dictionary, only process dict values  
        business_search_path = 'rows'
        all_businesses_jsonl = jmespath.search(business_search_path, all_businesses_json) 

        # Path should exist
        if all_businesses_jsonl is None:
            err_msg = f"{business_search_path} was not found in JSON."
            self.logger.error(err_msg)
            raise Exception(err_msg)
        
        # Should be dictionary
        if not isinstance(all_businesses_jsonl, dict):
            err_msg = f"Expected data format dict for {business_search_path}, but got {type(all_businesses_jsonl)}"
            self.logger.error(err_msg)
            raise Exception(err_msg)
        
        drawer_baseurl = 'https://firststop.sos.nd.gov/api/FilingDetail/business'
        drawer_headers = {
            "authorization" : "undefined",
            "Accept": "application/json",
        }

        # k contains business id and v contains more business definition 
        for _, (k, v) in tqdm(enumerate(all_businesses_jsonl.items())):
            v["ID_key"] = k
            business_id = v.get("ID", None) or k # use either ID attribute or key as ID.
            if business_id is None:
                warn_msg = f"ID does not exist for business id {business_id}"
                self.logger.warning(warn_msg)
            drawer_urlsuffix = f'{business_id}/false' # true or false in link return same result
            yield scrapy.Request(
                url=f"{drawer_baseurl}/{drawer_urlsuffix}",
                headers=drawer_headers,
                callback=self.parse_drawer_information,
                cb_kwargs={"business_meta" : v } # pass data to yield later
            )
	
    def parse_drawer_information(self, response, business_meta):
        '''
            Get information from drawer
        '''
        business_information = business_meta
        drawer_info_json = json.loads(response.text)
        if "DRAWER_DETAIL_LIST" in drawer_info_json:
                yield {
                    "DRAWER_DETAIL_LIST" : drawer_info_json["DRAWER_DETAIL_LIST"],
                    **business_information
                } 
        else:
            business_id = response.url.rsplit("/", 2)[0]
            warn_msg = f"DRAWER_DETAIL_LIST was not found for business id: {business_id}."
            self.logger.warning(warn_msg)
            yield business_information
