import csv
from datetime import datetime
from functools import cached_property

import os
import tempfile
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
        # return {"id": "frr_1S1CZWJAmnYVOvfn539yWWPr"}
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
        temp_file = None
        try:
            # Send GET request to the URL to fetch the CSV data with streaming
            headers = self.get_custom_headers()
            self.logger.info("Starting CSV download from Stripe...")
            
            response = requests.get(url, headers=headers, stream=True)
            self.validate_response(response)
            
            # Check content encoding and type
            content_encoding = response.headers.get('content-encoding', '').lower()
            content_type = response.headers.get('content-type', '').lower()
            
            self.logger.info(f"Response info - Content-Type: {content_type}, Content-Encoding: {content_encoding}")
            
            # Get the total file size from headers if available
            total_size = int(response.headers.get('content-length', 0))
            
            # Create a temporary file to store the CSV data
            temp_file = tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False)
            temp_file_path = temp_file.name
            
            self.logger.info(f"Temporary file created: {temp_file_path}")
            self.logger.info(f"Downloading CSV report ({total_size:,} bytes)..." if total_size > 0 else "📥 Downloading CSV report...")
            
            # Initialize progress tracking
            downloaded_size = 0
            chunk_size = 8192  # 8KB chunks
            last_logged_percent = -1
            
            # Download directly to file with progress indication
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # Filter out keep-alive chunks
                    temp_file.write(chunk)
                    downloaded_size += len(chunk)
                    
                    # Show progress only for whole number percentages
                    if total_size > 0:
                        current_percent = int((downloaded_size / total_size) * 100)
                        # Only log if we've reached a new whole number percentage
                        if current_percent > last_logged_percent and current_percent <= 100:
                            self.logger.info(f"⏳ Progress: {current_percent}% ({downloaded_size:,}/{total_size:,} bytes)")
                            last_logged_percent = current_percent
                    else:
                        # For unknown file size, log every MB downloaded
                        if downloaded_size % (1024 * 1024) < chunk_size:  # Every ~1MB
                            self.logger.info(f"⏳ Downloaded: {downloaded_size:,} bytes")
            
            # Close the temporary file so we can read from it
            temp_file.close()
            temp_file = None  # Reset to avoid double-close in finally block
            
            self.logger.info("Download completed! Opening CSV file...")
            
            # Open the file with proper encoding detection
            csv_file = self._open_csv_file_with_encoding_detection(temp_file_path, response.encoding)
            
            # Create a CSV DictReader from the file
            data = csv.DictReader(csv_file, delimiter=",")
            
            # Return a generator that yields rows and cleans up the temp file when done
            return self._csv_generator_with_cleanup(data, csv_file, temp_file_path)

        except requests.exceptions.RequestException as e:
            raise Exception(f"Error fetching CSV from URL: {e}")

        except csv.Error as e:
            raise Exception(f"Error parsing CSV data: {e}")
        
        except Exception as e:
            raise Exception(f"Unexpected error during CSV processing: {e}")
        
        finally:
            # Clean up resources
            if 'response' in locals():
                response.close()
            if temp_file is not None:
                temp_file.close()
    
    def _open_csv_file_with_encoding_detection(self, file_path, suggested_encoding):
        """Open CSV file with proper encoding detection."""
        encodings_to_try = []
        
        # Add suggested encoding first if available
        if suggested_encoding:
            encodings_to_try.append(suggested_encoding)
        
        # Add common encodings
        encodings_to_try.extend(['utf-8', 'latin-1', 'cp1252', 'iso-8859-1'])
        
        # Remove duplicates while preserving order
        encodings_to_try = list(dict.fromkeys(encodings_to_try))
        
        for encoding in encodings_to_try:
            try:
                self.logger.info(f"Trying to open file with encoding: {encoding}")
                csv_file = open(file_path, 'r', encoding=encoding)
                # Test read first few characters to validate encoding
                pos = csv_file.tell()
                csv_file.read(1024)
                csv_file.seek(pos)  # Reset to beginning
                self.logger.info(f"Successfully opened file with encoding: {encoding}")
                return csv_file
            except (UnicodeDecodeError, UnicodeError):
                continue
        
        # If all encodings fail, use UTF-8 with error replacement as last resort
        self.logger.info("Using UTF-8 with error replacement for problematic characters")
        return open(file_path, 'r', encoding='utf-8', errors='replace')
    
    def _csv_generator_with_cleanup(self, csv_reader, csv_file, temp_file_path):
        """Generator that yields CSV rows and cleans up temp file when done."""
        try:
            for row in csv_reader:
                yield row
        finally:
            # Clean up resources
            if csv_file:
                csv_file.close()
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.unlink(temp_file_path)
                    self.logger.info(f"Cleaned up temporary file: {temp_file_path}")
                except OSError as e:
                    self.logger.warning(f"Could not delete temporary file {temp_file_path}: {e}")

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
