from content import *


tables = pd.read_excel('~/Documents/Canvas Tables.xlsx')
for i,v in enumerate(tables.values):
    tbl_name = v[0].strip()
    print(tbl_name)
    canvas_dump = Get_schema(tbl_name)
    print(len(canvas_dump.get_data()))
    request_dump = canvas_dump.get_data()
    print(request_dump)
    # #Check table if exists in Data warehouse
    # if canvas_dump.check_db() is False:
    #     canvas_dump.create_table(schema_name='Canvas')
    # create_dump = canvas_dump.insert_table()

