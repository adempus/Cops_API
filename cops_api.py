from flask import Flask, jsonify, request
import pandas as pd

app = Flask(__name__)
df = pd.read_csv('./cpuo.csv')

functionMap = {
    'lessThan'   : 'lt',
    'greaterThan': 'gt',
    'equalTo'    : 'eq',
}

columnMap = {
    'rank':           'Ranking',
    'unit':           'Precinct/Unit',
    'numComplaints':  'Complaints Count',
    'numSubOfficers': 'Number Of Subject Officers'
}

# Should column be mapped to a column name returning corresponding value?
# or should it return all the values for a specified column in the query?
# column=Precinct/Unit |OR| Precinct/Unit=Housing Bureau
def getQueryKeys():
    year = int(request.args.get('year'))    # required
    column = str(request.args.get('column'))
    lt = str(request.args.get('lt'))
    gt = str(request.args.get('gt'))
    eq = str(request.args.get('eq'))

    return {'year': year, 'col': column ,
            'function': {'lt': lt, 'gt': gt, 'eq': eq }}

@app.route('/')
def index():
    head = "<h1> Cops Data API</h1>"
    usage = "<h3>Usage:<h3><br><h4>functions:<br>lt = less than, gt = greater than, eq = equals'</h4>"
    usage += "<h4>Columns: rank, unit, numComplaints, numSubOfficers <h4>"
    usage += "<h4><ul><li>rank : integer</li> <li>unit : string</li> <li>numComplaints : integer </li> <li>numSubOfficers : integer</li></ul><h4>"
    example = "<h4>Example: /get?year=2010&column=unit&eq=Patrol Borough Bronx</h4>"
    example += "<h4>Example: /get?year=2009&column=numComplaints&lt=250</h4>"
    return head+usage+example


@app.route('/get')
def getQuery():
    queryKeys = getQueryKeys()
    return runQueries(queryKeys)


def runQueries(queryKeys):
    ''' computes and returns results for valid queries in URL.'''
    initFrame = getFromYear(queryKeys['year'])
    column = queryKeys['col']
    funct = { k:v for k, v in queryKeys['function'].items() if v != 'None' }
    if initFrame is not None:
        return jsonify(getFromColumn(column, funct, initFrame).to_dict(orient='index'))


def getFromYear(year, sourceDF=None):
    ''' :param year: an integer representing a year in the data frame.
        :param sourceDF: a data frame provided to perform grouping on. None resorts to default data frame.
        :returns a DataFrame grouped by values of the specified year if it exists. None is returned for non.
    '''
    try:
        if sourceDF is None: return df.groupby('Year').get_group(year)
        else: return sourceDF.groupby('Year').get_group(year)
    except KeyError:
        return None


def getFromColumn(column, funct, sourceDF=None):
    ''' get from a specified column, a result from a particular function.
        :param column: the id of the column to query from the data frame
        :param funct: an action to perform on the provided column.
        :param sourceDF: data frame provided. If none, then default dataframe is used.
    '''
    if 'lt' in funct:
        ltv = int(funct['lt'])
        return sourceDF[sourceDF[columnMap[column]] < ltv]
    if 'gt' in funct:
        gtv = int(funct['gt'])
        return sourceDF[sourceDF[columnMap[column]] > gtv]
    if 'eq' in funct:
        if str(funct['eq']).isdigit():
            eqv = int(funct['eq'])
        else:
            eqv = str(funct['eq'])
        return sourceDF[sourceDF[columnMap[column]] == eqv]


if __name__ == '__main__':
    app.run()