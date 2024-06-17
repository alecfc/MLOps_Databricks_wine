from flask import Flask, render_template, request, redirect, url_for
import requests
import json

url = "https://adb-2310926350007386.6.azuredatabricks.net/serving-endpoints/alec_math/invocations"

app = Flask(__name__)



@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        # Process form data
        selected_wine = request.form['wine']
        selected_winery = request.form['winery']
        selected_category = request.form['category']
        selected_varietal = request.form['varietal']
        selected_alcohol_percentage = request.form['alcohol_percentage']
        selected_price = request.form['price']
        selected_country = request.form['country']
        selected_region = request.form['region']
        # You can add logic to process these inputs

        payload = json.dumps({
            "dataframe_split": {
                "columns": [
                "wine",
                "winery",
                "category",
                "designation",
                "varietal",
                "appellation",
                "alcohol",
                "price",
                "reviewer",
                "review",
                "country",
                "region"
                ],
                "data": [
                [
                    "wine",
                    "moli parallada",
                    "sparkling",
                    "designation",
                    "cava brut",
                    "",
                    11.5,
                    11,
                    "Mathijs",
                    "lekker bubbeltje, beetje te droog voor mij",
                    "Spain",
                    "Barcelona"
                ]
                ]
            }
            })
        headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic dG9rZW46ZGFwaTczYTRjYzMwY2Q0YWFmZmNiZDdhN2E5YTAyYTM1MTVmLTI='
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        # Use the json module to load CKAN's response into a dictionary.
        response_dict = json.loads(response.text)
        
        return redirect(url_for('results', wine=selected_wine, winery=selected_winery, category=selected_category,
                                        varietal=selected_varietal, alcohol_percentage=selected_alcohol_percentage,
                                        price=selected_price, country=selected_country, region=selected_region, rating=response_dict['predictions']))

    
    # Hardcoded options for the dropdowns
    wines = ['Wine1', 'Wine2', 'Wine3']
    wineries = ['Winery1', 'Winery2', 'Winery3']
    categories = ['Red', 'White', 'Rosé']
    varietals = ['Varietal1', 'Varietal2', 'Varietal3']
    alcohol_percentages = ['<12%', '12-14%', '14-16%', '>16%']
    prices = ['<10$', '10$-20$', '20$-30$', '>30$']
    countries = ['Country1', 'Country2', 'Country3']
    regions = ['Region1', 'Region2', 'Region3']
    
    return render_template('index.html', wines=wines, wineries=wineries, categories=categories, varietals=varietals, alcohol_percentages=alcohol_percentages, prices=prices, countries=countries, regions=regions)

@app.route('/results')
def results():
    wine = request.args.get('wine')
    winery = request.args.get('winery')
    category = request.args.get('category')
    varietal = request.args.get('varietal')
    alcohol_percentage = request.args.get('alcohol_percentage')
    price = request.args.get('price')
    country = request.args.get('country')
    region = request.args.get('region')
    rating = request.args.get('rating')

    
    return render_template('results.html', wine=wine, winery=winery, category=category,
                           varietal=varietal, alcohol_percentage=alcohol_percentage,
                           price=price, country=country, region=region, rating=rating)

if __name__ == '__main__':
    app.run(debug=True)
