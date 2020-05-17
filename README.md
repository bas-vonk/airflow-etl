## Local development

1. Fire up everything
2. `docker exec -it airflow-webserver bash`
3. `airflow initdb`
4. `airflow test extract_transform_load extract 2020-05-17 && airflow test extract_transform_load transform 2020-05-17 && airflow test extract_transform_load load 2020-05-17`

## Learnings
- ds parameter passes DateStamp when provide_context=True
- when provide_context=True --> \*\*kwargs will be greatly extended
