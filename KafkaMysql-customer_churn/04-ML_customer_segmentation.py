# Databricks notebook source
# MAGIC %md # Customer segmentation with SKlearn & MLFlow
# MAGIC 
# MAGIC In this example, we'll see how we can leverage MLFlow to create a user segmentation, analyze our clusters and save all the results to MLFlow:
# MAGIC 
# MAGIC - sklearn segmentation model
# MAGIC - "elbow graph" to find the ideal number of clusters
# MAGIC - cluster analysis to understand the population in each cluster (images/figures)
# MAGIC - cluster name & class based on our analysis
# MAGIC 
# MAGIC We'll then deploy this model in the registry, and use MLFlow to get back our clusters class!
# MAGIC 
# MAGIC *Note: use DBR 8.2 or mlflow 1.14.1+ to be able to log figures*

# COMMAND ----------

# DBTITLE 1,Let's import our libraries
# MAGIC %run ./ressources/setup

# COMMAND ----------

# DBTITLE 1,We'll be using our customer data and segment our customer population:
# MAGIC %sql
# MAGIC -- marwa_db is the target database defined in the dlt pipeline
# MAGIC select * from marwa_db.user_profile_and_behavior

# COMMAND ----------

# DBTITLE 1,Data exploration & visualization
customer_segmentation = spark.read.table("marwa_db.user_profile_and_behavior").toPandas()
sns.pairplot(customer_segmentation[['age','annual_income','spending_core']])

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC ### Customer Segmentation using KMeans
# MAGIC We'll be using KMeans with sklearn to segment our customers.
# MAGIC 
# MAGIC To pick the best number of cluster, we'll be using each classification inertia and draw an "elbow graph". The ideal number of cluster will be around the "elbow"

# COMMAND ----------

selected_cols = ['age','annual_income','spending_core']
cluster_data = customer_segmentation.loc[:,selected_cols]
scaler = MinMaxScaler([0,10])
cluster_scaled = scaler.fit_transform(cluster_data)

mlflow.set_experiment("/Users/marwa.krouma@databricks.com/DLT_Demo")

kmeans = []
#Enabling autolog to keep track of all our run automatically 
mlflow.sklearn.autolog(log_models=False)

with mlflow.start_run() as run:
  #We'll be testing from 2 to 10 clusters
  for cluster_number in range(2,10):
    #Each KMeans will be started as a sub-run to be properly logged in MLFlow
    with mlflow.start_run(nested=True):
      k = KMeans(n_clusters=cluster_number, random_state=0).fit(cluster_scaled)
      mlflow.log_metric("inertia", k.inertia_)
      kmeans.append(k)
      
  plt.plot([k.n_clusters for k in kmeans], [k.inertia_ for k in kmeans])
  plt.xlabel("Number of clusters")
  plt.ylabel("Inertia")
  mlflow.log_figure(plt.gcf(), "inertia.jpg")
  #Let's get back the run ID as we'll need to add other figures in our run from another cell
  run_id = run.info.run_id

# COMMAND ----------

# MAGIC %md ### Segmentation analysis
# MAGIC We now need to understand our segmentation and assign classes with some meaning to each cluster. Some visualization is required to do that, we'll display radars for each clusters

# COMMAND ----------

cluster_selected = 4
#let's get back the model having 4 clusters
selected_model = kmeans[cluster_selected-2]
#We can now run .predict() on our entire dataset to assign a cluster for each row: 
final_data = pd.DataFrame(cluster_scaled, columns=selected_cols)
final_data['cluster'] = selected_model.predict(cluster_scaled)

#Based on our prediction, let's analyze each cluster popupation using a radar for each cluster:
aggs = pd.DataFrame()
for col in selected_cols:
  aggs[col] = final_data.groupby("cluster")[col].mean()

radar_fig = make_subplots(rows=2, cols=2, specs=[[{'type': 'polar'}]*2]*2)
i = 0
for cluster_id, row in aggs.iterrows():
  radar_fig.add_trace(go.Scatterpolar(name = "Cluster "+str(cluster_id), r = row.tolist(), theta = selected_cols,), math.floor(i/2) + 1, i % 2 + 1)
  i = i+1
radar_fig.update_layout(height=800, width=1000, title_text="4 clusters exploration")
radar_fig.update_traces(fill='toself')

# COMMAND ----------

# MAGIC %md ### Updating our MLFlow run with the definition of our class and the radar figure

# COMMAND ----------


#Getting back the main run
with mlflow.start_run(run_id):
  #let's log our radar figure
  mlflow.log_figure(radar_fig, "radar_cluster.html")
  # log main model & params
  mlflow.log_param("n_clusters", selected_model.n_clusters)
  #Saving our model with the signature
  signature = infer_signature(final_data[selected_cols], final_data[['cluster']])
  mlflow.sklearn.log_model(selected_model, "kmeans", signature=signature)
  mlflow.set_tag("model", "kmeans")
  #match the clusters to a class. This could be done automatically with a small set of data labelled, or manually in this case
  clusters = {"cluster_1": "small_spenders", "cluster_2": "medium_spenders", "cluster_3": "large_spenders", "cluster_4": "critical_spenders"}
  mlflow.log_dict(clusters, "clusters_class.json")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### MLFlow now has all our model information and the model is ready to be deployed in our registry!
# MAGIC We can do that manually:
# MAGIC 
# MAGIC <img src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail-cdc-forecast/resources/images/mlflow_artifact.gif" alt="MLFlow artifacts"/>
# MAGIC 
# MAGIC or using MLFlow APIs directly:

# COMMAND ----------

# DBTITLE 1,Save our new model to the registry as a new version
#get the best model from the registry
best_model = mlflow.search_runs(filter_string='attributes.status = "FINISHED" and tags.model = "kmeans" and params.n_clusters = 4', max_results=1).iloc[0]
model_registered = mlflow.register_model("runs:/"+best_model.run_id+"/kmeans", "customer_segmentation_marwa")

# COMMAND ----------

# DBTITLE 1,Flag this version as production ready
client = mlflow.tracking.MlflowClient()
print("registering model version "+model_registered.version+" as production model")
client.transition_model_version_stage(name = "customer_segmentation_marwa", version = model_registered.version, stage = "Production", archive_existing_versions=True)

# COMMAND ----------

# MAGIC %md #Deploying & using our model in production
# MAGIC 
# MAGIC Now that our model is in our MLFlow registry, we can start to use it in a production pipeline.
# MAGIC 
# MAGIC We'll first see how it can be loaded as a spark UDF and called directly in a SQL function:

# COMMAND ----------

get_cluster_udf = mlflow.pyfunc.spark_udf(spark, "models:/customer_segmentation_marwa/production")
spark.udf.register("get_cluster", get_cluster_udf)

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, get_cluster(age,annual_income,spending_core) as segment from marwa_db.user_profile_and_behavior

# COMMAND ----------

# MAGIC %md ### That's a good first step, but what if we want to return the name of the class instead of the cluster id ?
# MAGIC 
# MAGIC We have stored the class name as an artefact with our model. Let's retrieve it and use it in our prediction!
# MAGIC 
# MAGIC Note that another solution could be to package this logic in the model itself and have a model in the registry returning the actual name of the class directly!

# COMMAND ----------

import mlflow
model = mlflow.pyfunc.load_model("models:/customer_segmentation_vivatech_demo/production")

# COMMAND ----------

# DBTITLE 1,Let's get back our classes from the model artefacts
#get back our model from registry
model = mlflow.pyfunc.load_model("models:/customer_segmentation_vivatech_demo/production")

#get the artifact clusters_class.json
client = MlflowClient()
with open(client.download_artifacts(model.metadata.run_id, "clusters_class.json")) as f:
  cluster_classes_name = json.load(f)

# COMMAND ----------

# DBTITLE 1,Custom pandas udf to return the name of the class instead of the cluster number
@pandas_udf("string")
def predict_category_udf(batch_iter: Iterator[Tuple[pd.Series, pd.Series, pd.Series]]) -> Iterator[pd.Series]:
  #Load the model in each executor, only once
  model = mlflow.pyfunc.load_model("models:/customer_segmentation_vivatech_demo/production")
  #For each batch, apply transformation
  for age, annual_income, spending_core in batch_iter:
    df = pd.DataFrame({ 'age': age, 'annual_income': annual_income, 'spending_core': spending_core } )
    predictions = model.predict(df)
    classes = pd.Series(predictions).apply(lambda x: cluster_classes_name['cluster_'+str(x+1)])
    yield classes

# COMMAND ----------

spark.udf.register("predict_category", predict_category_udf)
df = spark.sql("select *, predict_category(age,annual_income,spending_core) as segment from marwa_db.user_profile_and_behavior ")
df.write.mode("overwrite").saveAsTable("marwa_db.user_profile_and_behavior_results")

# COMMAND ----------

# MAGIC %md #### Or using pure python and pandas direcly in a single node for smaller dataset:

# COMMAND ----------

model = mlflow.pyfunc.load_model("models:/customer_segmentation_vivatech_demo/production")
df = spark.sql("select age, annual_income, spending_core from marwa_db.user_profile_and_behavior limit 10").toPandas()
df['cluster'] = model.predict(df)
df['cluster_name'] = df['cluster'].apply(lambda x: cluster_classes_name['cluster_'+str(x+1)])
df

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Our data is ready! Let's create a dashboard to understand our Customer segmentation results for churn analysis
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC [Open SQL Analytics dashboard](https://e2-demo-west.cloud.databricks.com/sql/dashboards/5bf37bd3-3bbd-483b-91b8-cf8faa48dc1c-customer_segmentation-churn?o=2556758628403379)

# COMMAND ----------


