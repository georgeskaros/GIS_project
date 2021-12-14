import psycopg2
import psycopg2.extras
import pandas as pd
import ast
import os


# print('Connecting with DB...')
# con = psycopg2.connect(database="taxi_db",
#                       user="postgres",
#                       password="admin",
#                       host="localhost",
#                       port="5432")
# print('Connected')
# print('Fetching main table...')
# traj_sql = "SELECT * FROM TRAIN limit 10;"
# traj_df = pd.read_sql(traj_sql, con)

# print(traj_df.head(10))

# print(traj_df.loc[traj_df['missing_data'] == True])


def parseCSV(path):
    entries = []
    with open(path, 'r') as f:
        # Read the CSV lines
        lines = [line.strip() for line in f.readlines() if line.strip()]
        # Isolate the CSV columns
        columns = [column.strip('"') for column in lines[0].split(',')]
        # Extract the CSV data
        for line in lines[1:]:
            entry = {}
            values = [value.strip('"') for value in line.split('","')]
            for i in range(len(columns)):
                if columns[i] == 'POLYLINE':
                    entry[columns[i]] = ast.literal_eval(values[i])
                else:
                    entry[columns[i]] = values[i]
            entries.append(entry)
    return entries


def lineToPoints(df):
    col = list(df.columns)
    retDF = pd.DataFrame(columns=col)
    retDF["LATITUDE"] = ""
    retDF["LONGITUDE"] = ""
    k = 0
    for j in range(len(df)):
        print(j)
        for i in range(len(df.iloc[j]['POLYLINE'])):
            retDF.at[k, 0:4] = df.iloc[j, 0:4]
            retDF.at[k, 5] = int(df.iloc[j]['TIMESTAMP']) + (15*i)
            retDF.at[k, 6:7] = df.iloc[j, 6:7]
            retDF.at[k, 8] = df.iloc[j]['POLYLINE'][i][0]
            retDF.at[k, 9] = df.iloc[j]['POLYLINE'][i][1]
            k = k + 1
    return retDF


print('Opening csv...')
entry = parseCSV('D:/unipi/THESIS/TEST_PAPER/35kPoints.csv')


dff = pd.DataFrame(entry)
dff = dff.drop_duplicates(subset=['TRIP_ID'])

print('Making it into a data frame...')
pointsDF = lineToPoints(dff)

print('Dataframe made with length ',  len(pointsDF))
pointsDF = pointsDF.reset_index()
pointsDF = pointsDF.drop(['index'], axis=1)


print('Creating csv...')
pointsDF.drop(['POLYLINE'], axis=1).to_csv(r'D:/unipi/THESIS/TEST_PAPER/35ktrips.csv', index=False, header=True)
print('Csv created')

