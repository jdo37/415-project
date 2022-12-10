import PySimpleGUI as sg
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable, ClientError
import math
import time

uri = "bolt://localhost:7687"
user = "neo4j"
#password for neo4j database
password = ""
categories = ['Sports', 'Film & Animation', 'News & Politics', 'Entertainment', 'Comedy', 'Music', 'Howto & DIY', 'Pets & Animals', 'People & Blogs', 'Autos & Vehicles', 
            'Gadgets & Games', 'Travel & Places', ' UNA ']
categories_buttons = ['Sports', 'Film && Animation', 'News && Politics', 'Entertainment', 'Comedy', 'Music', 'Howto && DIY', 'Pets && Animals', 'People && Blogs', 'Autos && Vehicles', 
            'Gadgets && Games', 'Travel && Places', ' UNA ']
views = [(0, 10000), (10000, 20000), (20000, 30000), (30000, 40000), (40000, 50000), (50000, 60000), (60000, 70000), (70000, 80000), (80000, 90000), (90000, 100000), 
        (100000, 200000), (200000, 300000), (300000, 400000), (400000, 500000), (500000, 600000), (600000, 700000), (700000, 800000), (800000, 900000), (900000, 1000000),
         (1000000, 10000000)]
ages = [(0, 100), (100, 200), (200, 300), (300, 400), (400, 500), (500, 600), (600, 700), (700, 800), (800, 900), (900, 1000)]
rates = [(0.0, 0.99), (1.0, 1.99), (2.0, 2.99), (3.0, 3.99), (4.0, 4.99), (5.0, 5.0)]
lengths = [(0, 100), (100, 200), (200, 300), (300, 400), (400, 500), (500, 600), (600, 700), (700, 800), (800, 900), (900, 1000), (1000, 1100), (1100, 1200), (1200, 1300),
            (1300, 1400), (1400, 1500), (1500, 1600), (1600, 1700), (1700, 1800), (1800, 1900), (1900, 2000), (2000, 3000), (3000, 4000)]

class Query:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()

    #finds all videos in a category
    def find_category_videos(self, category):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_category_videos, category)
            return result

    @staticmethod
    def _find_and_return_category_videos(tx, category):
        query = (
            "MATCH (:Category {name: $name})-->(video) "
            "RETURN video.id AS id"
        )
        result = tx.run(query, name=category)
        return [row["id"] for row in result]

    #finds all videos in a category with veiws in range
    def find_category_videos_in_range(self, category, lower, upper):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_category_videos_in_range, category, lower, upper)
            return result

    @staticmethod 
    def _find_and_return_category_videos_in_range(tx, category, lower, upper):
        query = (
            "MATCH (:Category {name: $name})-->(v) "
            "WHERE v.views >= $lower AND v.views < $upper "
            "RETURN v.id AS id, v.category AS category, v.length AS length, v.views AS views, v.rate AS rate, v.age AS age "
            "ORDER BY v.views DESC, v.rate DESC"
        )
        result = tx.run(query, name=category, lower=lower, upper=upper)
        return [[row["id"], row["category"], row["length"], row["views"], row["rate"], row["age"]] for row in result]

    #find all videos with views in range
    def find_views_in_range(self, lower, upper):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_views_in_range, lower, upper)
            return result

    @staticmethod
    def _find_and_return_views_in_range(tx, lower, upper):
        query = (
            "MATCH (v:Video) "
            "WHERE v.views >= $lower AND v.views < $upper "
            "RETURN v.id AS id, v.category AS category, v.length AS length, v.views AS views, v.rate AS rate, v.age AS age "
            "ORDER BY v.views DESC, v.rate DESC"
        )
        result = tx.run(query, lower=lower, upper=upper)
        return [[row["id"], row["category"], row["length"], row["views"], row["rate"], row["age"]] for row in result]

    #finds all videos with length in range
    def find_lengths_in_range(self, lower, upper):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_lengths_in_range, lower, upper)
            return result

    @staticmethod
    def _find_and_return_lengths_in_range(tx, lower, upper):
        query = (
            "MATCH (v:Video) "
            "WHERE v.length >= $lower AND v.length < $upper "
            "RETURN v.id AS id, v.category AS category, v.length AS length, v.views AS views, v.rate AS rate, v.age AS age "
            "ORDER BY v.length DESC, v.views DESC, v.rate DESC"
        )
        result = tx.run(query, lower=lower, upper=upper)
        return [[row["id"], row["category"], row["length"], row["views"], row["rate"], row["age"]] for row in result]

    #finds a videos with rate in range
    def find_rates_in_range(self, lower, upper):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_rates_in_range, lower, upper)
            return result

    @staticmethod
    def _find_and_return_rates_in_range(tx, lower, upper):
        query = (
            "MATCH (v:Video) "
            "WHERE v.rate >= $lower AND v.rate < $upper "
            "RETURN v.id AS id, v.category AS category, v.length AS length, v.views AS views, v.rate AS rate, v.age AS age "
            "ORDER BY v.rate DESC, v.views DESC"
        )
        result = tx.run(query, lower=lower, upper=upper)
        return [[row["id"], row["category"], row["length"], row["views"], row["rate"], row["age"]] for row in result]

    #find all videos with age in range
    def find_ages_in_range(self, lower, upper):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_ages_in_range, lower, upper)
            return result

    @staticmethod
    def _find_and_return_ages_in_range(tx, lower, upper):
        query = (
            "MATCH (v:Video) "
            "WHERE v.age >= $lower AND v.age < $upper "
            "RETURN v.id AS id, v.category AS category, v.length AS length, v.views AS views, v.rate AS rate, v.age AS age "
            "ORDER BY v.age DESC, v.views DESC, v.rate DESC"
        )
        result = tx.run(query, lower=lower, upper=upper)
        return [[row["id"], row["category"], row["length"], row["views"], row["rate"], row["age"]] for row in result]

    #finds all videos, specifying views
    def find_all_views(self):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_all_views)
            return result

    @staticmethod
    def _find_and_return_all_views(tx):
        query = (
            "MATCH (v:Video) "
            "RETURN v.id AS id, v.views AS views "
            "ORDER BY v.views"
        )
        result = tx.run(query)
        return [(row["id"], row["views"]) for row in result]

    #finds all videos, specifying rate
    def find_all_rates(self):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_all_rates)
            return result

    @staticmethod
    def _find_and_return_all_rates(tx):
        query = (
            "MATCH (v:Video) "
            "RETURN v.id as id, v.rate AS rate "
            "ORDER BY v.rate"
        )
        result = tx.run(query)
        return [(row["id"], row["rate"]) for row in result]

    #finds all videos, specifying age
    def find_all_ages(self):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_all_ages)
            return result

    @staticmethod
    def _find_and_return_all_ages(tx):
        query = (
            "MATCH (v:Video) "
            "RETURN v.id as id, v.age AS age "
            "ORDER BY v.age"
        )
        result = tx.run(query)
        return [(row["id"], row["age"]) for row in result]

    #finds all videos, specifying length
    def find_all_lengths(self):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_all_lengths)
            return result

    @staticmethod
    def _find_and_return_all_lengths(tx):
        query = (
            "MATCH (v:Video) "
            "RETURN v.id as id, v.length AS length "
            "ORDER BY v.length"
        )
        result = tx.run(query)
        return [(row["id"], row["length"]) for row in result]

    def find_page_rank(self):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_page_rank)
            return result

    @staticmethod
    def _find_and_return_page_rank(tx):
        query = (
            "CALL gds.pageRank.stream('myGraph') "
            "YIELD nodeId, score "
            "RETURN gds.util.asNode(nodeId).id AS id, "
            "gds.util.asNode(nodeId).category AS category, "
            "gds.util.asNode(nodeId).length AS length, "
            "gds.util.asNode(nodeId).views AS views, "
            "gds.util.asNode(nodeId).rate AS rate, "
            "gds.util.asNode(nodeId).age AS age, score "
            "ORDER BY score DESC"
        )
        result = tx.run(query)
        return [[row["id"], row["category"], row["length"], row["views"], row["rate"], row["age"], row["score"]] for row in result]

    def find_degree_centrality(self):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_read(self._find_and_return_degree_centrality)
            return result

    @staticmethod
    def _find_and_return_degree_centrality(tx):
        query = (
            "CALL gds.degree.stream('myGraph') "
            "YIELD nodeId, score "
            "RETURN gds.util.asNode(nodeId).id AS id, "
            "gds.util.asNode(nodeId).category AS category, "
            "gds.util.asNode(nodeId).length AS length, "
            "gds.util.asNode(nodeId).views AS views, "
            "gds.util.asNode(nodeId).rate AS rate, "
            "gds.util.asNode(nodeId).age AS age, score AS related "
            "ORDER BY related DESC, id DESC"
        )
        result = tx.run(query)
        return [[row["id"], row["category"], row["length"], row["views"], row["rate"], row["age"], "", row["related"]] for row in result]

    def create_graph_projection(self):
        with self.driver.session(database="neo4j") as session:
            session.execute_read(self._create_graph_projection)

    @staticmethod
    def _create_graph_projection(tx):
        query = (
            "CALL gds.graph.project( "
            "'myGraph', "
            "'Video', "
            "'RELATEDTO'"
            ")"
        )
        tx.run(query)

#gets video frequency, partitioned by category
def get_category_freq(categories, query):
    temp = []
    for category in categories:
        temp.append([category, len(query.find_category_videos(category))])
    return reversed(sorted(temp, key = lambda x: x[1]))

#gets video frequency, partitioned by views
def get_views_in_ranges(query):
    nodes = query.find_all_views()
    index = 0
    count = 0
    temp = []
    for node in nodes:
        if node[1] >= views[index][0] and node[1] < views[index][1]:
            count += 1
        else:
            temp.append([views[index], count])
            index += 1
            count = 1
    temp.append([views[index], count])
    mapped = map((lambda x: ["[" + f"{x[0][0]:,}" + " - " + f"{x[0][1]:,}" + "]", x[1]]), temp)
    return mapped

#gets video frequency, partitioned by rate
def get_rates(query):
    nodes = query.find_all_rates()
    index = 0
    count = 0
    temp = []
    for node in nodes:
        if (math.isclose(node[1], rates[index][0]) or node[1] > rates[index][0]) and (math.isclose(node[1], rates[index][1]) or node[1] < rates[index][1]):
            count += 1
        else:
            temp.append([rates[index], count])
            index += 1
            count = 1
    temp.append([rates[index], count])
    last = temp.pop()
    mapped = list(map((lambda x: ["[" + str(x[0][0]) + " - " + str(x[0][1]) + "]", x[1]]), temp))
    mapped.append(["[" + str(last[0][0]) + "]", last[1]])
    return mapped

#gets video frequency, partitioned by age
def get_ages(query):
    nodes = query.find_all_ages()
    index = 0
    count = 0
    temp = []
    for node in nodes:
        if node[1] >= ages[index][0] and node[1] < ages[index][1]:
            count += 1
        else:
            temp.append([ages[index], count])
            index += 1
            count = 1
    temp.append([ages[index], count])
    mapped = map((lambda x: ["[" + f"{x[0][0]:,}" + " - " + f"{x[0][1]:,}" + " days]", x[1]]), temp)
    return list(mapped)

#gets video frequency, partitioned by length
def get_lengths(query):
    nodes = query.find_all_lengths()
    index = 0
    count = 0
    temp = []
    for node in nodes:
        if node[1] >= lengths[index][0] and node[1] < lengths[index][1]:
            count += 1
        else:
            temp.append([lengths[index], count])
            index += 1
            count = 1
    temp.append([lengths[index], count])
    mapped = map((lambda x: ["[" + f"{x[0][0]:,}" + " - " + f"{x[0][1]:,}" + "]", x[1]]), temp)
    return list(mapped)

def limit(list, k):
    return list[:k]

def degree_stat(nodes):
    mapped = map((lambda x: x[7]), nodes)
    total = sum(list(mapped))
    avg = total / len(nodes)
    max = nodes[0][7]
    min = nodes[-1][7]
    return [["Max Degree", max], ["Min Degree", min], ["Average", avg]]


query = Query(uri, user, password)

#only need to run once when database is opened
try:
    query.create_graph_projection()
except:
    print("Graph Projection already created")

headings = ["ID", "Category", "Length", "Views", "Rate", "Age", "Rank", "Degree"]
button_menu = ['Select', ['Category', [categories_buttons], 'Length', 'Views', 'Rates', 'Age']]
limit_menu = [5, 10, 25, 50, 100, 250, 500, 1000, "All"]
layout = [
    [sg.Button("Category", size =(10, 1)), sg.Button("Length", size =(10, 1)), sg.Button("Views", size =(10, 1)), 
    sg.Button("Rates", size =(10, 1)), sg.Button("Age", size =(10, 1)), 
    sg.Button("Page Rank", size =(10, 1)), sg.Button("Degree", size =(10, 1)), 
    sg.OptionMenu(values=limit_menu, default_value="All", key="-LIMIT-"), sg.Button("Close")],
    
    [sg.ButtonMenu('Button Menu', menu_def=button_menu, key="-B_MENU-",), sg.Text('Lower Bound:', size =(10, 1)), 
    sg.InputText(size=(10,1), key="-T_LOWER-"), sg.Text('Upper Bound:', size =(10, 1)), sg.InputText(size=(10,1), 
    key="-T_UPPER-"), sg.Button("Find", size =(5, 1))],
    
    [sg.Table(values=[], headings=["Group", "Frequency"], def_col_width=15, auto_size_columns=False, display_row_numbers=False, justification='center', row_height=35, key="-FILE LIST-"), 
    sg.Table(values=[], headings=headings, max_col_width=35, auto_size_columns=False, display_row_numbers=False, justification='center', row_height=35, key='-TABLE-')]
]

window = sg.Window("Analyzer",  layout)

while True:
    event, values = window.read()
    # End program if user closes window
    video_limit = values["-LIMIT-"]
    if event == "Category":
        window["-FILE LIST-"].update(get_category_freq(categories, query))
    elif event == "Length":
        window["-FILE LIST-"].update(get_lengths(query))
    elif event == "Views":
        window["-FILE LIST-"].update(get_views_in_ranges(query))
    elif event == "Rates":
        window["-FILE LIST-"].update(get_rates(query))
    elif event == "Age":
        window["-FILE LIST-"].update(get_ages(query))
    elif event == "Page Rank":
        result = query.find_page_rank()
        if video_limit != "All":
                result = limit(result, int(video_limit))
        window["-TABLE-"].update(result)
    elif event == "Degree":
        result = query.find_degree_centrality()
        window["-FILE LIST-"].update(degree_stat(result))
        if video_limit != "All":
                result = limit(result, int(video_limit))
        window["-TABLE-"].update(result)
    elif event == "Find":
        selected = "Sports"
        lower = 0
        upper = 1000000000
        if values["-B_MENU-"] != None:
            selected = values["-B_MENU-"]
        if values["-T_LOWER-"] != "":
            lower = values["-T_LOWER-"]
        if values["-T_UPPER-"] != "":
            upper = values["-T_UPPER-"]
        if selected in categories:
            result = query.find_category_videos_in_range(selected, int(lower), int(upper))
            if video_limit != "All":
                result = limit(result, int(video_limit))
            window["-TABLE-"].update(result)
        elif selected == "Length":
            result = query.find_lengths_in_range(int(lower), int(upper))
            if video_limit != "All":
                result = limit(result, int(video_limit))
            window["-TABLE-"].update(result)
        elif selected == "Views":
            result = query.find_views_in_range(int(lower), int(upper))
            if video_limit != "All":
                result = limit(result, int(video_limit))
            window["-TABLE-"].update(result)
        elif selected == "Rates":
            result = query.find_rates_in_range(float(lower), float(upper))
            if video_limit != "All":
                result = limit(result, int(video_limit))
            window["-TABLE-"].update(result)
        elif selected == "Age":
            result = query.find_ages_in_range(int(lower), int(upper))
            if video_limit != "All":
                result = limit(result, int(video_limit))
            window["-TABLE-"].update(result)
    if event == "Close" or event == sg.WIN_CLOSED:
        break

window.close()
query.close()
