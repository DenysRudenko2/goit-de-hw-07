from airflow import DAG
from datetime import datetime, timedelta
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr
import random
import time

"""
IMPORTANT: This DAG requires a MySQL connection in Airflow with the following configuration:
Connection Id: goit_mysql_db_rudenko
Connection Type: MySQL
Host: 217.61.57.46
Schema: olympic_dataset
Login: neo_data_admin
Password: Proyahaxuqithab9oplp
Port: 3306
"""

# Default DAG arguments
default_args = {
    'owner': 'rudenko',
    'start_date': datetime(2024, 11, 26),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# MySQL connection configuration
CONN = "goit_mysql_db_rudenko"  # Connection name in Airflow
SCHEMA = "olympic_dataset"  # Using existing schema
TABLE = "rudenko_medal_counts"  # Table with rudenko prefix

DELAY_SECONDS = 10  # Normal delay (sensor will succeed)
FRESH_LIMIT = 30  # 30 seconds limit for sensor

def pick_medal():
    """Randomly selects one of three medal types"""
    medal = random.choice(['Bronze', 'Silver', 'Gold'])
    print(f"Selected medal: {medal}")
    # Return the corresponding task_id for branching
    return f"calc_{medal}"

def delay_func():
    """Creates a delay between tasks"""
    print(f"Starting delay for {DELAY_SECONDS} seconds...")
    time.sleep(DELAY_SECONDS)
    print("Delay completed")

# Define the DAG
with DAG(
    'rudenko_olympic_medals_dag',  # Single clear DAG name
    default_args=default_args,
    schedule_interval=None,  # Manual execution only
    catchup=False,  # Don't run missed tasks
    tags=['rudenko', 'hw-07', 'olympic', 'medals']  # Tags for easy identification
) as dag:
    
    # Task 1: Create table (matches homework schema)
    create_table = MySqlOperator(
        task_id="create_table",
        mysql_conn_id=CONN,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10) NOT NULL,
            count INT NOT NULL,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
    )

    # Task 2: Pick medal - PythonOperator that returns a value
    pick_medal_task = PythonOperator(
        task_id="pick_medal",
        python_callable=pick_medal,
    )

    # Task 3: Pick medal task - BranchPythonOperator for branching
    pick_medal_task_branch = BranchPythonOperator(
        task_id="pick_medal_task",
        python_callable=lambda ti: ti.xcom_pull(task_ids='pick_medal'),
    )

    # Task 4.1: Calculate Bronze medals
    calc_Bronze = MySqlOperator(
        task_id="calc_Bronze",
        mysql_conn_id=CONN,
        sql=f"""
        INSERT INTO {SCHEMA}.{TABLE} (medal_type, count, created_at)
        SELECT 'Bronze', COUNT(*), NOW()
        FROM {SCHEMA}.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )
    
    # Task 4.2: Calculate Silver medals
    calc_Silver = MySqlOperator(
        task_id="calc_Silver",
        mysql_conn_id=CONN,
        sql=f"""
        INSERT INTO {SCHEMA}.{TABLE} (medal_type, count, created_at)
        SELECT 'Silver', COUNT(*), NOW()
        FROM {SCHEMA}.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )
    
    # Task 4.3: Calculate Gold medals
    calc_Gold = MySqlOperator(
        task_id="calc_Gold",
        mysql_conn_id=CONN,
        sql=f"""
        INSERT INTO {SCHEMA}.{TABLE} (medal_type, count, created_at)
        SELECT 'Gold', COUNT(*), NOW()
        FROM {SCHEMA}.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    # Task 5: Generate delay
    generate_delay = PythonOperator(
        task_id="generate_delay",
        python_callable=delay_func,
        trigger_rule=tr.NONE_FAILED_MIN_ONE_SUCCESS,  # Run if at least one calc succeeded
    )

    # Task 6: Check for correctness - sensor to verify record freshness
    check_for_correctness = SqlSensor(
        task_id="check_for_correctness",
        conn_id=CONN,
        sql=f"""
        SELECT
          CASE
            WHEN TIMESTAMPDIFF(SECOND, MAX(created_at), NOW()) <= {FRESH_LIMIT}
            THEN 1 
            ELSE 0 
          END AS is_recent
        FROM {SCHEMA}.{TABLE};
        """,
        mode="poke",  # Check periodically
        poke_interval=5,  # Check every 5 seconds
        timeout=60,  # Timeout after 60 seconds
        soft_fail=False,  # Will fail if condition not met
    )
    
    # Define task dependencies (matching homework schema)
    create_table >> pick_medal_task >> pick_medal_task_branch
    pick_medal_task_branch >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay >> check_for_correctness