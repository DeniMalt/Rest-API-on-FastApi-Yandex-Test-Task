from flask import Flask, request
from flask_restful import Api, Resource
from flask import render_template
import psycopg2
import json
from validators import validator_of_time, validator_data_of_courier, validator_data_of_orders, validator_of_completed_order, validator_data_for_rating
from validators import limiter


app = Flask(__name__)
api = Api()
conn = psycopg2.connect(user="postgres", password="tonykart", database="dt_for_yandex")
conn.autocommit = True

try:
    with conn.cursor() as cursor:
        cursor.execute(
            """CREATE TABLE orders(
            id numeric,
            price numeric,
            time text,
            weight numeric,
            district numeric,
            id_courier numeric,
            condition text,
            execution_time timestamp);""")
except:
    pass

try:
    with conn.cursor() as cursor:
        cursor.execute(
            """CREATE TABLE couriers(
            id numeric,
            time text,
            district numeric,
            type text,
            rating numeric);""")
except:
    pass

@app.errorhandler(429)
def error_429():
    return render_template('HTTP 429 Exceeded the number of requests'), 429


@app.errorhandler(400)
def error_400():
    return render_template('HTTP 400 Bad Request'), 400


class get_orders_with_limit_offset(Resource):
    def get(self, limit, offset):
        name = "get_orders_with_limit_offset"
        if limiter(name) == False:
            return error_429()
        with conn.cursor() as cursor:
            cursor.execute(
                f"""select * from orders limit {limit} offset {offset}"""
            )
            response = cursor.fetchall()
            if len(response) == 0:
                return json.dumps({"orders": []})
            else:
                valide_list = []
                for i in range(len(response)):
                    try:
                        valide_list.append({"id": int(response[i][0]), "price": float(response[i][1]), "time": response[i][2], "weight": float(response[i][3]), "district": int(response[i][4]),
                                            "id_courier": int(response[i][5]), "condition": response[i][6], "execution_time": str(response[i][7])})
                    except TypeError:
                        valide_list.append({"id": int(response[i][0]), "price": float(response[i][1]), "time": response[i][2],
                                            "weight": float(response[i][3]), "district": int(response[i][4]),
                                            "id_courier": response[i][5], "condition": response[i][6],
                                            "execution_time": str(response[i][7])})
                return json.dumps({"orders": valide_list})


class get_orders(Resource):
    def get(self):
        name = "get_orders"
        if limiter(name) == False:
            return error_429()
        with conn.cursor() as cursor:
            cursor.execute(
                f"""select * from orders limit 1 offset 0"""
            )
            response = cursor.fetchall()
            valide_list = []
            if len(response) == 0:
                return json.dumps({"orders": []})
            else:
                for i in range(len(response)):
                    try:
                        valide_list.append({"id": int(response[i][0]), "price": float(response[i][1]), "time": response[i][2],
                                            "weight": float(response[i][3]), "district": int(response[i][4]),
                                            "id_courier": int(response[i][5]), "condition": response[i][6],
                                            "execution_time": response[i][7]})
                    except TypeError:
                        valide_list.append({"id": int(response[i][0]), "price": float(response[i][1]), "time": response[i][2],
                                            "weight": float(response[i][3]), "district": int(response[i][4]),
                                            "id_courier": response[i][5], "condition": response[i][6],
                                            "execution_time": response[i][7]})
                return json.dumps(valide_list)


class get_order_by_id(Resource):
    def get(self, id):
        name = "get_order_by_id"
        if limiter(name) == False:
            return error_429()
        with conn.cursor() as cursor:
            cursor.execute(
                f"""select * from orders where id = {id}"""
            )
            response = cursor.fetchall()
            if len(response) == 0:
                return json.dumps({"order": []})
            else:
                valide_list = []
                try:
                    valide_list.append({"id": int(response[0][0]), "price": float(response[0][1]), "time": response[0][2],
                                        "weight": float(response[0][3]), "district": int(response[0][4]),
                                        "id_courier": int(response[0][5]), "condition": response[0][6],
                                        "execution_time": response[0][7]})
                except TypeError:
                    valide_list.append({"id": int(response[0][0]), "price": float(response[0][1]), "time": response[0][2],
                                        "weight": float(response[0][3]), "district": int(response[0][4]),
                                        "id_courier": response[0][5], "condition": response[0][6],
                                        "execution_time": response[0][7]})
                return json.dumps(valide_list)


class insert_couriers(Resource):
    def post(self):
        name = "insert_couriers"
        if limiter(name) == False:
            return error_429()
        data = request.get_json(force=True)
        with conn.cursor() as cursor:
            cursor.execute("""select id from couriers""")
            list_ids_of_couriers = cursor.fetchall()
        list_ids_of_couriers_valide = []
        for i in range(len(list_ids_of_couriers)):
            list_ids_of_couriers_valide.append(int(list_ids_of_couriers[i][0]))
        if validator_data_of_courier(data, list_ids_of_couriers_valide) == True:
            for j in range(len(data['couriers'])):
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"""insert into couriers values ({data['couriers'][j]['id']}, '{data['couriers'][j]['time']}', {data['couriers'][j]['district']}, '{data['couriers'][j]['type']}');"""
                    )


class get_courier_by_id(Resource):
    def get(self, id_courier):
        name = "get_courier_by_id"
        if limiter(name) == False:
            return error_429()
        with conn.cursor() as cursor:
            cursor.execute(
                f"""select * from couriers where id = {id_courier}"""
            )
            data_of_courier = cursor.fetchall()
            if len(data_of_courier) == 0:
                return json.dumps({"courier": []})
            if validator_of_time(data_of_courier[0][1][:5]) == True and validator_of_time(data_of_courier[0][1][6:]) == True and len(data_of_courier) != 0:
                try:
                    data_of_courier_valide = {"id": int(data_of_courier[0][0]), "time": str(data_of_courier[0][1]), "district": int(data_of_courier[0][2]),
                                   "type": str(data_of_courier[0][3]), "rating": float(data_of_courier[0][4])}
                except TypeError:
                    data_of_courier_valide = {"id": int(data_of_courier[0][0]), "time": str(data_of_courier[0][1]),
                                              "district": int(data_of_courier[0][2]),
                                              "type": str(data_of_courier[0][3]), "rating": data_of_courier[0][4]}
                return json.dumps(data_of_courier_valide)


class get_couriers(Resource):
    def get(self):
        name = "get_couriers"
        if limiter(name) == False:
            return error_429()
        with conn.cursor() as cursor:
            cursor.execute(
                """select * from couriers offset 0 limit 1"""
            )
            data_of_couriers = cursor.fetchall()
        data_of_couriers_valide = []
        if len(data_of_couriers) == 0:
            return json.dumps({"couriers": []})
        else:
            for i in range(len(data_of_couriers)):
                try:
                    data_of_couriers_valide.append(
                        {"id": int(data_of_couriers[i][0]), "time": data_of_couriers[i][1], "district": int(data_of_couriers[i][2]), "type": data_of_couriers[i][3],
                         "rating": float(data_of_couriers[i][4])})
                except TypeError:
                    data_of_couriers_valide.append({"id": int(data_of_couriers[i][0]), "time": data_of_couriers[i][1], "district": int(data_of_couriers[i][2]), "type": data_of_couriers[i][
                        3], "rating": data_of_couriers[i][4]})
            return json.dumps(data_of_couriers_valide)


class get_couriers_with_limit_and_offset(Resource):
    def get(self, limit, offset):
        name = "get_couriers_with_limit_and_offset"
        if limiter(name) == False:
            return error_429()
        with conn.cursor() as cursor:
            cursor.execute(
                f"""select * from couriers limit {limit} offset {offset}"""
            )
            data_of_couriers = cursor.fetchall()
        data_of_couriers_valide = []
        if len(data_of_couriers) == 0:
            return json.dumps({"couriers": []})
        else:
            for i in range(len(data_of_couriers)):
                try:
                    data_of_couriers_valide.append(
                        {"id": int(data_of_couriers[i][0]), "time": data_of_couriers[i][1], "district": int(data_of_couriers[i][2]), "type": data_of_couriers[i][3],
                         "rating": float(data_of_couriers[i][4])})
                except TypeError:
                    data_of_couriers_valide.append({"id": int(data_of_couriers[i][0]), "time": data_of_couriers[i][1], "district": int(data_of_couriers[i][2]), "type": data_of_couriers[i][
                        3], "rating": data_of_couriers[i][4]})
            return json.dumps(data_of_couriers_valide)


class insert_orders(Resource):
    def post(self):
        name = "insert_orders"
        if limiter(name) == False:
            return error_429()
        data = request.get_json(force=True)
        with conn.cursor() as cursor:
            cursor.execute(
                """select id from orders"""
            )
            ids_orders_from_db = cursor.fetchall()

        ids_orders_from_db_valide = []
        for j in range(len(ids_orders_from_db)):
            ids_orders_from_db_valide.append(int(ids_orders_from_db[j][0]))

        if validator_data_of_orders(data, ids_orders_from_db_valide) == True:
            for k in range(len(data['orders'])):
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"""insert into orders values 
                        ({data['orders'][k]['id']}, {data['orders'][k]['price']}, '{data['orders'][k]['time']}', {data['orders'][k]['weight']}, {data['orders'][k]['district']}, null,
                        'in progress');"""
                    )


class change_condition_of_orders(Resource):
    def post(self):
        name = "change_condition_of_orders"
        if limiter(name) == False:
            return error_429()
        data = request.get_json(force=True)
        list_ids_of_orders_from_db_valide = []
        with conn.cursor() as cursor:
            cursor.execute(
                """select id, id_courier from orders where condition = 'in progress'"""
            )
            l = cursor.fetchall()
            for i in range(len(l)):
                try:
                    list_ids_of_orders_from_db_valide.append({"id_order": int(l[i][0]), "id_courier": int(l[i][1])})
                except TypeError:
                    list_ids_of_orders_from_db_valide.append({"id_order": int(l[i][0]), "id_courier": l[i][1]})
        if validator_of_completed_order(data, list_ids_of_orders_from_db_valide) == True:
            for j in range(len(data['orders_completed'])):
                with conn.cursor() as cursor:
                    cursor.execute(
                        f"""update orders set id_courier = {data['orders_completed'][j]['id_courier']}, condition = 'completed', execution_time = '{data['orders_completed'][j]['time']}' where id = {data['orders_completed'][j]['id_order']}"""
                    )
        else:
            return error_400()


class rating_and_wages_of_courier(Resource):
    def get(self, id):
        name = "rating_and_wages_of_courier"
        if limiter(name) == False:
            return error_429()
        data = request.get_json()
        with conn.cursor() as cursor:
            cursor.execute(
                f"""select count(*) from orders where id_courier = {id} and condition = 'completed' and '{data['interval'][0]['start_date']}' <= execution_time and execution_time < '{data['interval'][0]['end_date']}'"""
            )
            count_delivers = cursor.fetchall()
            count_delivers_valide = int(count_delivers[0][0])

            if count_delivers_valide != 0 and validator_data_for_rating(data) == True:
                cursor.execute(
                    f"""select extract(epoch from ('{data['interval'][0]['end_date']}'::timestamp without time zone - '{data['interval'][0]['start_date']}'::timestamp without time zone))/60/60"""
                )
                count_of_hours = cursor.fetchall()
                count_of_hours_valide = int(count_of_hours[0][0])
                cursor.execute(
                    f"""select type from couriers where id = {id}"""
                )
                type_of_courier = cursor.fetchall()
                type_of_courier_valide = type_of_courier[0][0]
                rating_of_courier = 0
                if type_of_courier_valide == 'bike_courier':
                    rating_of_courier += (count_delivers_valide / count_of_hours_valide) * 2
                if type_of_courier_valide == 'foot_courier':
                    rating_of_courier += (count_delivers_valide / count_of_hours_valide) * 3
                if type_of_courier_valide == 'courier_on_auto':
                    rating_of_courier += (count_delivers_valide / count_of_hours_valide) * 1

                cursor.execute(
                    f"""select price from orders where id_courier = {id} and condition = 'completed' and '{data['interval'][0]['start_date']}' <= execution_time and execution_time < '{data['interval'][0]['end_date']}'"""
                )
                list_of_prices = cursor.fetchall()
                list_of_prices_valide = []
                for i in range(len(list_of_prices)):
                    list_of_prices_valide.append(float(list_of_prices[i][0]))
                wages_courier = 0
                if type_of_courier_valide == 'bike_courier':
                    for j in range(len(list_of_prices_valide)):
                        wages_courier += list_of_prices_valide[j] * 3
                if type_of_courier_valide == 'foot_courier':
                    for j in range(len(list_of_prices_valide)):
                        wages_courier += list_of_prices_valide[j] * 2
                if type_of_courier_valide == 'courier_on_auto':
                    for j in range(len(list_of_prices_valide)):
                        wages_courier += list_of_prices_valide[j] * 4
                return json.dumps({"rating": rating_of_courier, "wages": wages_courier})
            else:
                return None


api.add_resource(get_orders_with_limit_offset, "/orders/limit=<int:limit>/offset=<int:offset>")
api.add_resource(get_orders, "/orders")
api.add_resource(get_order_by_id, "/orders/<int:id>")
api.add_resource(insert_couriers, "/couriers")
api.add_resource(get_courier_by_id, "/couriers/<int:id_courier>")
api.add_resource(get_couriers, "/couriers")
api.add_resource(get_couriers_with_limit_and_offset, "/couriers/limit=<int:limit>/offset=<int:offset>")
api.add_resource(insert_orders, "/orders")
api.add_resource(change_condition_of_orders, "/orders/complete")
api.add_resource(rating_and_wages_of_courier, "/couriers/meta-info/<int:id>")
api.init_app(app)

if __name__ == '__main__':
    app.run(debug=True, port=8080, host="127.0.0.1")
