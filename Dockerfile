FROM quay.io/astronomer/astro-runtime:12.2.0
# RUN python -m venv dbt_venv && \
#     source dbt_venv/bin/activate && \
#     pip install --no-cache-dir dbt-bigquery==1.8.3 && \
#     deactivate

# RUN source dbt_venv/bin/activate && dbt deps > /tmp/dbt_deps_output.log

# Activate the virtual environment before running dbt commands
# WORKDIR source dbt_venv/bin/activate  
# RUN source dbt_venv/bin/activate && dbt deps

# install soda into a virtual environment
# RUN python -m venv soda_venv && source soda_venv/bin/activate && \
#     pip install --no-cache-dir soda-core-bigquery==3.0.45 &&\
#     pip install --no-cache-dir soda-core-scientific==3.0.45 && deactivate

# install dbt into a virtual environment
# install dbt into a virtual environment
# RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
#     pip install --no-cache-dir dbt-bigquery==1.7.1 && deactivate

    # install dbt into a virtual environment
# RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
#     pip install --no-cache-dir dbt-core==1.5.3 dbt-bigquery==1.5.3 && deactivate

    
# install dbt into a virtual environment
# RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
#     pip install --no-cache-dir dbt-bigquery==1.5.3 && deactivate  

