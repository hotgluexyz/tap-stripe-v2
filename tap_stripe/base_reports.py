import csv
from datetime import datetime
from functools import cached_property
from io import StringIO
import time
from typing import Any, Dict, Iterable, Optional
import requests
from dateutil.parser import parse
from tap_stripe.client import stripeStream


class BaseReportsStream(stripeStream):
    def get_custom_headers(self):
        headers = self.http_headers
        # get the headers with auth token populated
        auth_headers = self.authenticator.auth_headers
        headers.update(auth_headers)
        return headers

    def get_report_ranges(self):
        """We can request stripe for available data ranges for a given report type
        This is safer option because we will always get a valid response for valid ranges.
        Otherwise stripe will raise an error.
        Returns:
            available starting and ending date ranges.
        """
        url = f"{self.url_base}reporting/report_types/{self.report_type}"
        resp = requests.get(url=url, headers=self.get_custom_headers())
        self.validate_response(resp)
        data = resp.json()
        return data["data_available_start"], data["data_available_end"]

    @cached_property
    def selected_properties(self):
        selected_properties = []
        for key, value in self.metadata.items():
            if isinstance(key, tuple) and len(key) == 2 and value.selected:
                field_name = key[-1]
                selected_properties.append(field_name)
        return selected_properties

    def create_report(self, start_date, end_date):
        url = f"{self.url_base}reporting/report_runs"
        headers = self.get_custom_headers()
        body = {}
        # The report data and processing time will vary based on report type and requested interval.
        body["report_type"] = self.report_type
        body["parameters[interval_start]"] = start_date
        body["parameters[interval_end]"] = end_date
        body = list(body.items())
        # Not ready for production
        for column in self.selected_properties:
            body.append(("parameters[columns][]", column))

        # Make the request
        response = requests.post(url=url, headers=headers, data=body)
        self.validate_response(response)
        data = response.json()
        return data

    def verify_report(self, report_id):
        res = {}
        # keep checking for report status until report is ready for download
        while True:
            headers = self.get_custom_headers()
            url = f"{self.url_base}reporting/report_runs/{report_id}"
            response = requests.get(url, headers=headers)
            self.validate_response(response)
            data = response.json()
            # Stripe will return processing status in "status" property of the response
            if data["status"] == "succeeded" and "result" in data:
                res = data["result"]["url"]
                break
            # wait for 30 seconds before checking again
            time.sleep(30)
        return res

    def read_csv_from_url(self, url):
        try:
            # Send GET request to the URL to fetch the CSV data
            headers = self.get_custom_headers()
            response = requests.get(url, headers=headers)
            self.validate_response(response)
            csv_file = StringIO(response.text)

            # Create a CSV DictReader from the response content
            data = csv.DictReader(csv_file, delimiter=",")
            return data

        except requests.exceptions.RequestException as e:
            raise (f"Error fetching CSV from URL: {e}")

        except csv.Error as e:
            raise (f"Error parsing CSV data: {e}")

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        for field in self.datetime_fields:
            if row.get(field):
                # Payout stream have valid formatted dates instead of unix timestamp
                dt_field = parse(row[field])
                if isinstance(dt_field, datetime):
                    row[field] = dt_field.isoformat()
        return row

    def get_records(self, context: Optional[dict]) -> Iterable[Dict[str, Any]]:
        """Stripe provides a CSV report that can be used to get all records.
        to get all the records we need to do the following steps:
        1. Create/request a report
        2. Periodically check status of the report if its done.
        3. Download the CSV and process. it
        """
        # @TODO use this only if there is no incremental state present.
        start_date, end_date = self.get_report_ranges()
        report = self.create_report(start_date, end_date)
        if report.get("result") is not None:
            # This means report was already processed and download url is already available
            report_file = report["result"]["url"]
        else:
            """
            If a report in given range and type is already requested stripe will return previously created report's detail.
            In this case will start verifying the report.
            """
            report_file = self.verify_report(report["id"])
        records = self.read_csv_from_url(report_file)
        for record in records:
            transformed_record = self.post_process(record, context)
            if transformed_record is None:
                # Record filtered out during post_process()
                continue
            yield transformed_record
