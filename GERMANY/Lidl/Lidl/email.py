import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pymysql

# List of database connections
database_connections = [
    {'name': 'Eleclerc', 'host': 'localhost', 'user': 'root', 'password': 'actowiz', 'database': 'eleclerc','table1':'spirit_data_05012023','table2':'spirit_data_06112023'},
    # {'name': 'firehousesubs', 'host': 'localhost', 'user': 'root', 'password': 'actowiz', 'database': 'firehousesubs','table1':'firehousesubs_data_done'},
    # Add more databases as needed
]

# Specify the table name for which you want to get the row count
# target_table = 'your_table_name'
# -------------------------------------
# table_header
table_header = (
        "<table border='1' class='dataframe'>"
        "<tr>"
        "<th rowspan='2'>Project_name</th>"
        "<th colspan='3'>Project_Count</th>"
        "</tr>"
        "<tr>"
        "<th>present delivery count</th>"
        "<th>past delivery count</th>"
        "<th>Difference</th>"
        "</tr>"
    )
# -----------------------------
li_row = []
for db_connection in database_connections:
    connection_params = {
        'host': db_connection['host'],
        'user': db_connection['user'],
        'password': db_connection['password'],
        'database': db_connection['database'],
        # 'table': db_connection['table'],
    }
    try:
        target_table1 = db_connection['table1']

        database_name = db_connection['name']

        # Connect to the database
        connection = pymysql.connect(**connection_params)
        cursor = connection.cursor()

        # Get row count for the specified table
        cursor.execute(f"SELECT COUNT(*) FROM {target_table1}")
        row_count_1 = cursor.fetchone()[0]
        connection.close()
    except:
        database_name = db_connection['name']
        row_count_1 = 0
    try:
        target_table2 = db_connection['table2']

        database_name_2 = db_connection['name']

        # Connect to the database
        connection = pymysql.connect(**connection_params)
        cursor = connection.cursor()

        # Get row count for the specified table
        cursor.execute(f"SELECT COUNT(*) FROM {target_table2}")
        row_count_2 = cursor.fetchone()[0]
        connection.close()
    except:
        database_name_2 = db_connection['name']
        row_count_2 = 0
    # -------------------------------------------------
    new_data_row = (
        "<tr>"
        f"<td>{database_name}</td>"
        f"<td>{row_count_1}</td>"
        f"<td>{row_count_2}</td>"
        f"<td>{row_count_1 - row_count_2}</td>"
        "</tr>"
    )
    li_row.append(new_data_row)

    # ----------------------------------------------------
table_closing_tags = "</table>"
final_table = table_header + ''.join(li_row) + table_closing_tags

print(final_table)


from_email = 'hinalp.actowiz@gmail.com'
li = ['meetsa.actowiz@gmail.com']
cc= ['hinalpatel3401@gmail.com']

# for i in li:

to_email = ', '.join(li)


# to_email = 'abhishekp.actowiz@gmail.com'
subject = f'Project Data'
message = ''

# Create email message
msg = MIMEMultipart()
msg['hinalp.actowiz@gmail.com'] = from_email
msg['To'] = to_email
msg['subject'] = subject
msg['Cc'] = ', '.join(cc)
msg.attach(MIMEText(message, 'plain'))

msg.attach(MIMEText(final_table, 'html'))
# msg.attach(MIMEText(xyz01, 'html'))

# Send email
smtp_server = 'smtp.gmail.com'
smtp_port = 587
smtp_username = 'hinalp.actowiz@gmail.com'
smtp_password = 'zwgi zylu njml divy'

with smtplib.SMTP(smtp_server, smtp_port) as server:
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.sendmail(from_email, to_email, msg.as_string())

print('Email sent successfully!')