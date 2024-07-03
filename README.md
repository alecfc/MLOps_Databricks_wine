# MLOps in Databricks workshop
This repository is meant as a demo for implementing MLOps practices in Databricks with MLFlow. Using the code in this repository, we will do this by looking at the use case of accurately predicting wine price!

Steps to run AutoML experiments and track models with MLFlow:
1. Go to the Databricks URL that was shared with you and open your workspace
2. Click on Create -> Git folder
3. Choose Github as provider and paste the repository URL: https://github.com/alecfc/MLOps_Databricks_wine.git
4. The first step is to setup the database and tables. Go to database_setup and run all cells (make sure to use your own name for my_name)
5. Go to Catalog and check if your database and tables are there.
6. Go back to your workspace and open data_preprocessing. Follow the steps in this notebook.
7. Some data preprocessing steps can be improved. We don't have much time to implement these improvements, but can you think of 3 possible improvements to the train and test data?
8. Go back to Catalog and look at your train and test tables' Sample Data. These should now be filled with data!
9. It is now time to run an AutoML experiment using your train data. Go to Experiments -> Create AutoML Experiment.
10. In the Configuration Pane, choose the right ML problem, data, columns and prediction target.
11. Go to advanced and put the Evaluation Metric on Mean-Absolute Error. **Also make sure to set the timeout at 10 minutes.** Finally, start the experiment!
12. Answer the second question
13. After 2-3 minutes, a button to view the data exploration notebook should appear. Click on it and explore the data using the auto-generated notebook and answer question 3, 4 and 5.
14. Wait until the experiment has finished and view and register the best performing model in MLFLow with a new model name.
15. The model should have a Source notebook. Click on this notebook to view how the model is trained.
16. Answer questions 6 and 7>
17. Let's test put our model to the test. In order to do this, you need to put the model into the Staging stage. Go to Models -> your model name -> Version 1. Finally click on Stage and Transition to Staging
18. Now click on Use model for inference. Select Batch inference and choose the test table as test data and run your test.
19. In the newly opened notebook, run all cells and look at the output dataframe in the last cell. How are the predictions of your model compared to the actual price? When is there a large difference and why?
20. For the final steps you will serve your model behind an endpoint and communicate . First go back to your model and select Use model for inference again.
21. In the Real-time tab, create an endpoint with a similar name to your model and make sure to have Compute set to small.
22. It will take a couple of minutes before your endpoint is ready. When it is, copy the URL and go to this website: https://winebrightcubes.azurewebsites.net/. In the website, paste your URL and fill in all the details for the bottle of wine before clicking Submit.
23. The next page will load with a prediction of the price of the bottle of wine!
24. With MLFlow, you now have an easily accessible model that can be used for both real-time and batch inference. Next possible steps would be to retrain your existing model with new data, creating a new version. But considering the time we have, we believe you deserve a drink!
