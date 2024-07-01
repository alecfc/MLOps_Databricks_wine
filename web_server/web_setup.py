from flask import Flask, render_template, request, redirect, url_for
import requests
import json
import pandas as pd
import zipfile

app = Flask(__name__)


def get_field_values():
    categories = ['Red', 'White', 'Sparkling', 'Rose', 'Dessert', 'Port/Sherry', 'Fortified']
    countries = ['US', 'Chile', 'Spain', 'France', 'Italy', 'Portugal', 'Australia', 'South Africa', 'Argentina', 'Germany', 'Austria', 'Israel', 'New Zealand', 'Greece', 'Romania', 'Hungary']
    regions = [' California',
        ' Washington',
        ' Tuscany',
        ' Oregon',
        ' Bordeaux',
        ' Northern Spain',
        ' Burgundy',
        ' Piedmont',
        ' Mendoza Province',
        ' Veneto',
        ' Rhône Valley',
        ' Alsace',
        ' South Australia',
        ' New York',
        ' Loire Valley']
    zf = zipfile.ZipFile('/data/wine_data_zip') 
    df = pd.read_csv(zf.open('wine_first_batch.csv'))
    return available_categories, available_countries, available_regions




@app.route('/', methods=['GET', 'POST'])
def index():
    available_categories, available_countries, available_regions = get_field_values()

    if request.method == 'POST':
        # Process form data
        selected_url = request.form['model_url']
        selected_year = int(request.form['year'])
        selected_winery = request.form['winery']
        selected_category = request.form['category']
        selected_varietal = request.form['varietal']
        selected_alcohol_percentage = request.form['alcohol_percentage']
        selected_country = request.form['country']
        selected_region = request.form['region']
        # You can add logic to process these inputs
        if selected_alcohol_percentage == '':
            selected_alcohol_percentage = 12

        payload = json.dumps({
            "dataframe_split": {
                "columns": [
                "id",
                "year",
                "wine",
                "winery",
                "category",
                "wine_name",
                "designation",
                "grape_variety",
                "appellation",
                "alcohol",
                "rating",
                "reviewer",
                "review",
                "country",
                "region"
                ],
                "data": [
                [
                    None,
                    selected_year,
                    None,
                    selected_winery,
                    selected_category,
                    None,
                    None,
                    selected_varietal,
                    None,
                    selected_alcohol_percentage,
                    85,
                    None,
                    None,
                    selected_country,
                    selected_region
                ]
                ]
            }
            })
        headers = {
        'Content-Type': 'application/json',
        'Authorization': "Bearer dapicbe66e05adbe91737c4d2d4d33114a8f-2"
        }

        response = requests.request("POST", selected_url, headers=headers, data=payload)
        # Use the json module to load CKAN's response into a dictionary.
        response_dict = json.loads(response.text)
        return redirect(url_for('results', winery=selected_winery, category=selected_category, year=str(selected_year),
                                        varietal=selected_varietal, alcohol_percentage=selected_alcohol_percentage,
                                        country=selected_country, region=selected_region, price=response_dict['predictions']))

    
    return render_template('index.html', categories=categories, alcohol_percentages=alcohol_percentages, countries=countries, regions=regions)

@app.route('/results')
def results():
    year = request.args.get('year')
    winery = request.args.get('winery')
    category = request.args.get('category')
    varietal = request.args.get('varietal')
    alcohol_percentage = request.args.get('alcohol_percentage')
    price = request.args.get('price')
    country = request.args.get('country')
    region = request.args.get('region')
    rating = request.args.get('rating')

    
    return render_template('results.html', year=year, winery=winery, category=category,
                           varietal=varietal, alcohol_percentage=alcohol_percentage,
                           price=price, country=country, region=region, rating=rating)

if __name__ == '__main__':
    app.run(debug=True)
