from router import router
import uvicorn
from fastapi import FastAPI, Body, Header, HTTPException
import time
from jsonschema import ValidationError
import datetime
import psycopg2
import json
from router import validator_of_time, validator_data_of_courier, isnumeric, validator_data_of_orders, validator_of_completed_order, validator_data_for_rating
from router import limiter

conn = psycopg2.connect(user="postgres", password="password")
conn.autocommit = True

def get_application() -> FastAPI:
    application = FastAPI()
    application.include_router(router)

    return application


app = get_application()

def create_and_insert_orders():
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE orders(
                id numeric,
                price numeric,
                delivery_hours text,
                weight numeric,
                regions numeric,
                id_courier numeric,
                condition text,
                complete_time timestamp);""")
    except:
        pass

    with conn.cursor() as cursor:
        cursor.execute(
            """INSERT INTO orders VALUES
            (2, 3090, '14:00-20:00', 2000, 5, null, 'in progress', null),
            (45, 3095, '11:00-19:30', 2100, 1, null, 'in progress', null),
            (15, 2090, '14:00-22:00', 1000, 3, 88, 'completed', '2023-01-09 14:15:30'),
            (653, 5090, '09:00-21:00', 500, 1, null, 'in progress', null),
            (3, 3090, '14:00-20:00', 2000, 1, null, 'in progress', null),
            (34, 1090, '09:00-20:00', 2145, 1, 46, 'in progress', null),
            (37, 590, '11:00-12:00', 2008, 4, null, 'in progress', null),
            (396, 290, '14:00-20:00', 2000, 1, null, 'in progress', null),
            (3907, 23090, '10:00-19:00', 700, 3, 88, 'completed', '2023-02-06 18:35:42'),
            (1, 340, '10:00-21:00', 400, 4, 56, 'completed', '2023-03-05 12:34:26'),
            (5, 190, '11:00-14:00', 200, 4, 56, 'completed', '2023-03-05 13:23:39'),
            (6, 890, '08:00-12:00', 600, 3, 88, 'completed', '2022-12-23 14:40:21'),
            (12, 595, '14:00-16:00', 180, 3, 88, 'completed', '2022-12-12 15:45:12'),
            (46, 790, '12:00-13:35', 100, 1, null, 'in progress', null),
            (78, 309, '11:00-12:20', 50, 1, null, 'in progress', null),
            (7894, 390, '10:00-14:00', 150, 1, 46, 'in progress', null),
            (4768, 990, '10:00-20:00', 2000, 1, 46, 'completed', '2023-01-05 12:23:45'),
            (4769, 690, '11:00-11:40', 100, 1, 46, 'in progress', null),
            (4770, 790, '21:00-22:00', 120, 5, 78, 'in progress', null),
            (4771, 495, '14:00-15:00', 500, 5, 78, 'in progress', null),
            (4772, 400, '15:00-16:00', 200, 5, 78, 'in progress', null),
            (4773, 400, '14:00-14:30', 500, 5, 78, 'completed', '2022-10-01 12:34:45'),
            (4774, 995, '13:20-15:00', 600, 5, 78, 'completed', '2022-09-23 14:45:12'),
            (4775, 4950, '11:00-15:00', 1500, 2, null, 'in progress', null),
            (4776, 5490, '14:00-15:00', 900, 5, 78, 'completed', '2023-03-12 14:56:12'),
            (4777, 1490, '12:00-13:00', 1900, 5, 78, 'completed', '2023-03-12 12:30:12'),
            (4778, 2490, '17:00-19:00', 400, 5, 78, 'completed', '2023-03-12 17:54:05');"""
        )


def create_and_insert_couriers():
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """CREATE TABLE couriers(
                id numeric,
                time text,
                district numeric,
                type text);""")
    except:
        pass

    with conn.cursor() as cursor:
        cursor.execute(
            """INSERT INTO couriers VALUES
            (44, '14:00-23:00', 1, 'AUTO'),
            (88, '08:00-19:30', 3, 'BIKE'),
            (23, '13:00-22:00', 2, 'FOOT'),
            (46, '09:00-21:00', 1, 'AUTO'),
            (56, '09:00-21:00', 4, 'BIKE'),
            (78, '09:00-22:00', 5, 'AUTO'),
            (79, '11:00-18-00', 3, 'AUTO');"""
        )


@app.get("/orders/limit={limit}/offset={offset}")
async def get_orders_with_limit_offset(limit, offset):
    name = "get_orders_with_limit_offset"
    if limiter(name) == False:
        return HTTPException(status_code=429, detail="too many requests")
    with conn.cursor() as cursor:
        cursor.execute(
            f"""select * from orders limit {limit} offset {offset}"""
        )
        response = cursor.fetchall()
        if len(response) == 0:
            return {}
        else:
            valide_list = []
            for i in range(len(response)):
                valide_list.append(
                    {"order_id": int(response[i][0]), "weight": float(response[i][3]),
                     "regions": int(response[i][4]), "delivery_hours": [response[i][2]],
                     "cost": int(response[i][1]),
                     "completed_time": str(response[i][7])})

            return json.dumps(valide_list)


@app.get("/orders")
async def get_orders():
    name = "get_orders"
    if limiter(name) == False:
        return HTTPException(status_code=429, detail="Too many requests")
    with conn.cursor() as cursor:
        cursor.execute(
            f"""select * from orders limit 1 offset 0"""
        )
        response = cursor.fetchall()
        valide_list = []
        if len(response) == 0:
            return {}
        else:
            for i in range(len(response)):
                valide_list.append(
                    {"order_id": int(response[i][0]), "weight": float(response[i][3]),
                     "regions": int(response[i][4]), "delivery_hours": [response[i][2]],
                     "cost": int(response[i][1]),
                     "completed_time": str(response[i][7])})
            return json.dumps(valide_list)


@app.get("/orders/{id}")
async def get_order_by_id(id):
    name = "get_order_by_id"
    if limiter(name) == False:
        return HTTPException(status_code=429, detail="Too many requests")
    with conn.cursor() as cursor:
        cursor.execute(
            f"""select * from orders where id = {id}"""
        )
        response = cursor.fetchall()
        if len(response) == 0:
            return {}
        else:

            return json.dumps({"order_id": int(response[0][0]), "weight": float(response[0][3]),
                     "regions": int(response[0][4]), "delivery_hours": [response[0][2]],
                     "cost": int(response[0][1]),
                     "completed_time": str(response[0][7])})


@app.post("/couriers")
async def insert_couriers(data=Body()):
    name = "insert_couriers"
    if limiter(name) == False:
        return HTTPException(status_code=429, detail="Too many requests")
    with conn.cursor() as cursor:
        cursor.execute("""select id from couriers""")
        list_ids_of_couriers = cursor.fetchall()
    list_ids_of_couriers_valide = []
    for i in range(len(list_ids_of_couriers)):
        list_ids_of_couriers_valide.append(int(list_ids_of_couriers[i][0]))

    try:
        for j in range(len(data['couriers'])):
            data['couriers'][j]['courier_id'] = max(list_ids_of_couriers_valide) + j + 1
    except KeyError:
        return {}

    if validator_data_of_courier(data, list_ids_of_couriers_valide) == True:
        for j in range(len(data['couriers'])):
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""insert into couriers values ({data['couriers'][j]['courier_id']}, '{data['couriers'][j]['working_hours'][0]}', {data['couriers'][j]['regions'][0]}, '{data['couriers'][j]['courier_type']}');"""
                )
        return json.dumps(data)
    else:
        return {}


@app.get("/couriers/{id_courier}")
async def get_courier_by_id(id_courier):
    name = "get_courier_by_id"
    if limiter(name) == False:
        return HTTPException(status_code=429, detail="Too many requests")
    with conn.cursor() as cursor:
        cursor.execute(
            f"""select * from couriers where id = {id_courier}"""
        )
        data_of_courier = cursor.fetchall()
        if len(data_of_courier) == 0:
            return {}
        else:
            data_of_courier_valide = {"courier_id": int(data_of_courier[0][0]), "courier_type": str(data_of_courier[0][3]),
                                      "regions": [int(data_of_courier[0][2])],
                                      "working_hours": [str(data_of_courier[0][1])]}

            return json.dumps(data_of_courier_valide)


@app.get("/couriers")
async def get_couriers():
    name = "get_couriers"
    if limiter(name) == False:
        return HTTPException(status_code=429, detail="Too many requests")
    with conn.cursor() as cursor:
        cursor.execute(
            """select * from couriers offset 0 limit 1"""
        )
        data_of_couriers = cursor.fetchall()
    data_of_couriers_valide = []
    if len(data_of_couriers) == 0:
        return {}
    else:
        data_of_couriers_valide.append(
            {"courier_id": int(data_of_couriers[0][0]), "courier_type": str(data_of_couriers[0][3]),
             "regions": [int(data_of_couriers[0][2])],
             "working_hours": [str(data_of_couriers[0][1])]})
                
        return json.dumps({"couriers": data_of_couriers_valide, "limit": 1, "offset": 0})


@app.get("/couriers/limit={limit}/offset={offset}")
async def get_couriers_with_limit_and_offset(limit, offset):
    name = "get_couriers_with_limit_and_offset"
    if limiter(name) == False:
        return HTTPException(status_code=429, detail="Too many requests")
    with conn.cursor() as cursor:
        cursor.execute(
            f"""select * from couriers limit {limit} offset {offset}"""
        )
        data_of_couriers = cursor.fetchall()
    data_of_couriers_valide = []
    if len(data_of_couriers) == 0:
        return {}
    else:
        for i in range(len(data_of_couriers)):
            data_of_couriers_valide.append(
                {"courier_id": int(data_of_couriers[i][0]), "courier_type": str(data_of_couriers[i][3]),
                 "regions": [int(data_of_couriers[i][2])],
                 "working_hours": [str(data_of_couriers[i][1])]})

        return json.dumps({"couriers": data_of_couriers_valide, "limit": int(limit), "offset": int(offset)})


@app.post("/orders")
async def insert_orders(data=Body()):
    name = "insert_orders"
    if limiter(name) == False:
        return HTTPException(status_code=429, detail="Too many requests")
    with conn.cursor() as cursor:
        cursor.execute(
            """select id from orders"""
        )
        ids_orders_from_db = cursor.fetchall()

    ids_orders_from_db_valide = []
    for j in range(len(ids_orders_from_db)):
        ids_orders_from_db_valide.append(int(ids_orders_from_db[j][0]))

    try:
        for i in range(len(data['orders'])):
            data['orders'][i]['order_id'] = max(ids_orders_from_db_valide) + i + 1
    except KeyError:
        return {}

    if validator_data_of_orders(data, ids_orders_from_db_valide) == True:
        for k in range(len(data['orders'])):
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""insert into orders values 
                            ({data['orders'][k]['order_id']}, {data['orders'][k]['cost']}, '{data['orders'][k]['delivery_hours'][0]}', {data['orders'][k]['weight']}, {data['orders'][k]['regions']}, null,
                            'in progress');"""
                )
            data['orders'][k]['completed_time'] = None
        return json.dumps([i for i in data['orders']])
    else:
        return {}


@app.post("/orders/complete")
async def change_condition_of_orders(data=Body()):
    name = "change_condition_of_orders"
    if limiter(name) == False:
        return HTTPException(status_code=429, detail="Too many requests")
    list_ids_of_orders_from_db_valide = []
    with conn.cursor() as cursor:
        cursor.execute(
            """select id, id_courier from orders where condition = 'in progress'"""
        )
        l = cursor.fetchall()
        for i in range(len(l)):
            try:
                list_ids_of_orders_from_db_valide.append({"order_id": int(l[i][0]), "courier_id": int(l[i][1])})
            except TypeError:
                list_ids_of_orders_from_db_valide.append({"order_id": int(l[i][0]), "courier_id": l[i][1]})

    data_return_valide = []
    if validator_of_completed_order(data, list_ids_of_orders_from_db_valide) == True:
        for j in range(len(data['complete_info'])):
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""update orders set id_courier = {data['complete_info'][j]['courier_id']}, condition = 'completed', complete_time = '{data['complete_info'][j]['complete_time']}' where id = {data['complete_info'][j]['order_id']}"""
                )

            with conn.cursor() as cursor:
                cursor.execute(
                    f"""select * from orders where id = {data['complete_info'][j]['order_id']}"""
                )
                data_return = cursor.fetchall()
                data_return_valide.append({"order_id": int(data_return[0][0]), "weight": float(data_return[0][3]),
                                           "regions": int(data_return[0][4]), "delivery_hours": [data_return[0][2]], "cost": int(data_return[0][1]), "completed_time": str(data_return[0][7])})
        return json.dumps(data_return_valide)
    else:
        return {}


@app.get("/couriers/meta-info/id={id}/startDate={startDate}/endDate={endDate}")
async def rating_and_wages_of_courier(id, startDate, endDate):
    data = {
        'interval': [
            {
                "endDate": endDate,
                "startDate": startDate
            }
        ]
    }
    name = "rating_and_wages_of_courier"
    if limiter(name) == False:
        return HTTPException(status_code=429, detail="Too many requests")
    with conn.cursor() as cursor:
        cursor.execute(
            f"""select count(*) from orders where id_courier = {id} and condition = 'completed' and '{data['interval'][0]['startDate']}' <= complete_time and complete_time < '{data['interval'][0]['endDate']}'"""
        )
        count_delivers = cursor.fetchall()
        count_delivers_valide = int(count_delivers[0][0])

        if count_delivers_valide != 0 and validator_data_for_rating(data) == True:
            cursor.execute(
                f"""select extract(epoch from ('{data['interval'][0]['endDate']}'::timestamp without time zone - '{data['interval'][0]['startDate']}'::timestamp without time zone))/60/60"""
            )
            count_of_hours = cursor.fetchall()
            count_of_hours_valide = int(count_of_hours[0][0])
            cursor.execute(
                f"""select type from couriers where id = {id}"""
            )
            type_of_courier = cursor.fetchall()
            type_of_courier_valide = type_of_courier[0][0]
            rating_of_courier = 0
            if type_of_courier_valide == 'BIKE':
                rating_of_courier += (count_delivers_valide / count_of_hours_valide) * 2
            if type_of_courier_valide == 'FOOT':
                rating_of_courier += (count_delivers_valide / count_of_hours_valide) * 3
            if type_of_courier_valide == 'AUTO':
                rating_of_courier += (count_delivers_valide / count_of_hours_valide) * 1

            cursor.execute(
                f"""select price from orders where id_courier = {id} and condition = 'completed' and '{data['interval'][0]['startDate']}' <= complete_time and complete_time < '{data['interval'][0]['endDate']}'"""
            )
            list_of_prices = cursor.fetchall()
            list_of_prices_valide = []
            for i in range(len(list_of_prices)):
                list_of_prices_valide.append(float(list_of_prices[i][0]))
            wages_courier = 0
            if type_of_courier_valide == 'BIKE':
                for j in range(len(list_of_prices_valide)):
                    wages_courier += list_of_prices_valide[j] * 3
            if type_of_courier_valide == 'FOOT':
                for j in range(len(list_of_prices_valide)):
                    wages_courier += list_of_prices_valide[j] * 2
            if type_of_courier_valide == 'AUTO':
                for j in range(len(list_of_prices_valide)):
                    wages_courier += list_of_prices_valide[j] * 4

            cursor.execute(
                f"""select * from couriers where id = {id}"""
            )
            data_of_courier = cursor.fetchall()
            if len(data_of_courier) == 0:
                return {}

            if validator_of_time(data_of_courier[0][1][:5]) == True and validator_of_time(data_of_courier[0][1][6:]) == True and len(data_of_courier) != 0:
                data_of_courier_valide = {"courier_id": int(data_of_courier[0][0]),
                                          "courier_type": str(data_of_courier[0][3]),
                                          "regions": [int(data_of_courier[0][2])],
                                          "working_hours": [str(data_of_courier[0][1])],
                                          "rating": round(rating_of_courier), "earnings": round(wages_courier)}
            return json.dumps(data_of_courier_valide)

        else:
            return {}


if __name__ == '__main__':
    create_and_insert_orders()
    create_and_insert_couriers()
    uvicorn.run('main:app', host='127.0.0.1', port=8080)

