from requests_html import HTMLSession
from bs4 import BeautifulSoup
import re
import json
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

class ParseMindatPred:
    def __init__(self, URL):

        with HTMLSession() as session:
            #html_text = session.get(URL).text
            response = session.get(URL)
            #response.html.arender(timeout=20)
            html_text = response.text  

        self.id = re.findall(r'\d+$', URL)[0]  

        self.html_text = html_text
        # self.html_text = self.html_text.replace('\x00', '')
        self.soup = BeautifulSoup(self.html_text, "lxml")
        self.tables = self.soup.find_all('table', class_='mindattable')

        self.possible_minerals_list = []
        self.paragenetic_mode_list = []

        if [] == self.tables:
            return None
        
        elif 1 == len(self.tables):
            headers = self.tables[0].find_all('tr')[0]
            table_type = headers.find_all('th')[0].get_text().strip()
            if "Possible" in table_type:
                self.possible_minerals_list = self.get_pred_mineral_list(self.tables[0])
            elif "Mode" in table_type:
                self.paragenetic_mode_list = self.get_paragenetic_list(self.tables[0])
            else:
                raise Exception("Mine id: ", self.id, " table_type error")

        elif 2 == len(self.tables):
            self.possible_minerals_list = self.get_pred_mineral_list(self.tables[0])
            self.paragenetic_mode_list = self.get_paragenetic_list(self.tables[1])
        
        else:
            raise Exception("Mine id: ", self.id, " table length error")



 
    def get_pred_mineral_list(self, table):
        # Find the specific table, you might need to adjust the selector to target the right one
        # For example, if it's the first table with class 'mindattable', you would do:
        mineral_table = table

        # Initialize a list to hold all mineral names
        mineral_ids = []
        mineral_names = []
        mineral_matches = []

        # Assuming the first row is headers and data starts from the second row
        for row in mineral_table.find_all('tr')[1:]:  # Skip the header row
            # first_cell = row.find('td')  # Get the first cell of the row
            row_cells = row.find_all('td')
            if row_cells:
                # Find the <a> tag within the first cell
                a_tag = row_cells[0].find('a')
                
                # Extract the href attribute from the <a> tag
                if a_tag:
                    href_value = a_tag['href']
                else:
                    raise Exception("Failed to find <a> tag in the first cell")

                mineral_id = re.findall(r'\d+', href_value)[0]
                mineral_ids.append(str(mineral_id))

                try:
                    mineral_name = row_cells[0].get_text().strip()  # Extract text and strip whitespace
                    mineral_names.append(mineral_name)
                    mineral_match = row_cells[2].get_text().strip()
                    mineral_matches.append(mineral_match)
                except IndexError:
                    raise Exception("Mine id: ", self.id, " IndexError")

        possible_minerals_list = []
        for i in range(len(mineral_names)):
            temp_dict = {}
            temp_dict['mineral_id'] = mineral_ids[i]
            temp_dict['mineral_name'] = mineral_names[i]
            temp_dict['mineral_match'] = mineral_matches[i]
            possible_minerals_list.append(temp_dict)

        return possible_minerals_list

    def get_paragenetic_list(self, table):
        # Find the specific table, you might need to adjust the selector to target the right one
        # For example, if it's the first table with class 'mindattable', you would do:
        model_table = table

        # Initialize a list to hold all mineral names
        mode_ids = []
        mode_names = []
        mode_scores = []

        # Assuming the first row is headers and data starts from the second row
        for row in model_table.find_all('tr')[1:]:  # Skip the header row
            # first_cell = row.find('td')  # Get the first cell of the row
            row_cells = row.find_all('td')
            if row_cells:
                # Find the <a> tag within the first cell
                a_tag = row_cells[0].find('a')
                
                # Extract the href attribute from the <a> tag
                if a_tag:
                    href_value = a_tag['href']
                else:
                    raise Exception("Failed to find <a> tag in the first cell")
                mode_id = re.findall(r'\d+', href_value)[0]
                mode_ids.append(str(mode_id))

                try:
                    mode_name = row_cells[0].get_text().strip()  # Extract text and strip whitespace
                    mode_name = self.trim_mode_name(mode_name)
                    mode_names.append(mode_name)
                    mode_score = row_cells[1].get_text().strip()
                    mode_scores.append(mode_score)
                except IndexError:
                    raise Exception("Mine id: ", self.id, " IndexError")


        paragenetic_mode_list = []
        for i in range(len(mode_names)):
            temp_dict = {}
            temp_dict['mode_id'] = mode_ids[i]
            temp_dict['mode_name'] = mode_names[i]
            temp_dict['mode_score'] = mode_scores[i]
            paragenetic_mode_list.append(temp_dict)

        return paragenetic_mode_list


    def trim_mode_name(self, mode_name):
        unique_index = mode_name.find("Unique")
        if unique_index != -1:
            trimmed_sentence = mode_name[:unique_index]
            return trimmed_sentence
        else:
            return mode_name
        
    def get_table_json(self):
        temp_json = {}
        temp_json["id"] = self.id
        temp_json['possible_minerals'] = self.possible_minerals_list
        temp_json['paragenetic_modes'] = self.paragenetic_mode_list

        # table_json = json.dumps(temp_json, indent=4, sort_keys=True)
        return temp_json


class BatchPredDownloader:

    def __init__(self, FILENAME):
        self.FILENAME = FILENAME
        self.id_list = self.get_id_list()
    
    def get_id_list(self):
        id_list = []
        with open(self.FILENAME, 'r') as f:
            mine_data = json.load(f)
            for item in mine_data:
                id_list.append(str(item['id']))

        return id_list

    def extract_id_list(self):
        with open("extracted_kw_id_list.json", 'w') as f:
            json_temp = []
            for item in self.id_list:
                temp_dict = {}
                temp_dict['id'] = item
                json_temp.append(temp_dict)
            json.dump(json_temp, f, indent=4, sort_keys=True)
            

    def download_table(self, id):
        with open("url.txt", "r") as f:
            # Read the URL template from the file
            url_template = f.read().strip()

        # Format the URL with the ID
        url = url_template.format(id = id)

        pmp1 = ParseMindatPred(url)  # Assuming this is a defined class that parses the page
        if pmp1.tables:
            return pmp1.get_table_json()  # Assuming this returns JSON
        else:
            return None
    

    def is_internet_connected(self):
        """
        Check if the internet is connected
        """
        try:
            requests.get('http://google.com', timeout=3)
            return True
        except requests.ConnectionError:
            return False
        

    def download_all(self, max_workers=10):
        result_json = []
        missing_id = []
        progress_index = 0
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_id = {executor.submit(self.download_table, id): id for id in self.id_list}
            
            while future_to_id:
                # Check internet connection
                if not self.is_internet_connected():
                    print("Internet connection lost, pausing download...")
                    time.sleep(60)  # Wait 60 seconds before checking the connection again
                    continue

                for future in as_completed(future_to_id, timeout=9999999):  # Adjust timeout as needed
                    id = future_to_id.pop(future, None)
                    if not id:
                        continue

                    try:
                        data = future.result()
                        if data:
                            result_json.append(data)
                        else:
                            missing_id.append(id)
                    except Exception as e:
                        print(f"ID {id} generated an exception: {e}")
                        missing_id.append(id)

                    progress_index += 1
                    print(round(progress_index/len(self.id_list)*100, 2), "%, current missing id rate: ", round(len(missing_id)/progress_index*100, 2), "%")

        # Write the result to file
        with open("mindat_pred_result.json", 'w') as f:
            # f.write(str(result_json))
            json.dump(result_json, f, indent=4, sort_keys=True)
        
        if missing_id:
            with open("missing_id.json", 'w') as f:
                json_temp = []
                for item in missing_id:
                    temp_dict = {}
                    temp_dict['id'] = item
                    json_temp.append(temp_dict)
                json.dump(json_temp, f, indent=4, sort_keys=True)
            print("Missing id rate: ", round(len(missing_id)/len(self.id_list)*100, 2), "%")
    
        
        # Calculate elapsed time
        elapsed_time = time.time() - start_time
        print(f"Elapsed time: {elapsed_time} seconds")
        

if __name__ == "__main__":


    bpd1 = BatchPredDownloader("extracted_kw_id_list.json")

    bpd1.download_all()
