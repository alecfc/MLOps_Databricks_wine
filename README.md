# MLOps in Databricks workshop
This repository is meant as a demo for implementing MLOps practices in Databricks with MLFlow. Using the code in this repository, we will do this by looking at the use case of accurately predicting wine price!

Steps to run AutoML experiments and track models with MLFlow:
1. Go to the Databricks URL that was shared with you and click on "Workspace" (via the navigation pane on the left)
2. Click on Create -> Git folder
3. Choose Github as provider and paste the repository URL: https://github.com/alecfc/MLOps_Databricks_wine.git
4. The first step is to setup the database and tables. Go to database_setup and run all cells (make sure to use your own name for my_name)
5. Go to Catalog and check if your database and tables are there.
6. Go back to your workspace and open data_preprocessing. Follow the steps in this notebook.
7. Some data preprocessing steps can be improved. We don't have much time to implement these improvements, but can you think of 3 possible improvements to the train and test data? (answer question 1 on the questions form)
8. Go back to Catalog and look at your train and test tables' Sample Data. These should now be filled with data!
9. It is now time to run an AutoML experiment using your train data. Go to Experiments -> Create AutoML Experiment.
10. In the Configuration Pane, choose the right ML problem, data, columns and prediction target.
11. Go to advanced and put the Evaluation Metric on Mean-Absolute Error. **Also make sure to set the timeout at 10 minutes.** Finally, start the experiment!
12. Answer the second question on the questions form
13. After 2-3 minutes, a button "view data exploration notebook" should appear. Click on it and explore the data using the auto-generated notebook and answer question 3, 4 and 5.
14. Wait until the experiment has finished and click on the best performing model (below "Run name"). Then, hit "register model" (in the right upper corner), create a new model, choose a catchy model name, and save the model.
15. In the overview page of the experiment run, you see a table with details. Click on the notebook next to "Source" to view how the model is trained. Answer questions 6.
16. Scroll down towards "Feature Importance" and put shap_enabled on "True". Now rerun the complete notebook and answer question 7.
17. Let's test put our model to the test. In order to do this, you need to put the model into the Staging stage. Go to Models -> your model name -> Version 1. Finally click on Stage and Transition to Staging
18. Now click on Use model for inference. Select Batch inference and choose the test table as test data and run your test.
19. In the newly opened notebook, run all cells and look at the output dataframe in the last cell. Answer question 8.
20. We now consider our model "production ready". Change the stage of the model from Staging to Production.
21. For the final steps you will serve your model behind an endpoint and communicate. First go back to your model and select Use model for inference again.
22. In the Real-time tab, create an endpoint with a similar name to your model and make sure to have Compute set to small.
23. It will take 10-15 minutes before your endpoint is ready. You can take a break and grab a coffee and tea. When it is ready, copy the URL and go to this website: https://winebrightcubes.azurewebsites.net/. In the website, paste your URL and fill in all the details for the bottle of wine before clicking Submit.
24. The next page will load with a prediction of the price of the bottle of wine! Answer question 9 and hand the questions form over to Alec/Mathijs. The winner will be declared shortly!!!!!!
25. With MLFlow, you now have an easily accessible model that can be used for both real-time and batch inference. Next possible steps would be to retrain your existing model with new data, creating a new version. But considering the time we have, we believe you deserve a bottle of wine!
