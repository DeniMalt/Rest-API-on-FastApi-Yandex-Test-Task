import time
from jsonschema import ValidationError
import datetime


def isnumeric(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def validator_of_time(str1):
    try:
        time.strptime(str1, '%H:%M')
        return True
    except ValueError:
        return False


def validator_of_data(str1):
    if len(str1.split('-')) == 3:
        try:
            datetime.datetime.strptime(str1, '%d-%m-%Y')
            return True
        except Exception:
            return False
    else:
        try:
            datetime.datetime.strptime(str1, '%d-%m')
            return True
        except Exception:
            return False


def validator_data_of_courier(data_input, ids_from_db):
    values_of_type = ['courier_on_auto', 'bike_courier', 'foot_courier']
    count = 0
    try:
        if len(data_input["couriers"]) != 0:
            for i in range(len(data_input['couriers'])):
                if isinstance(data_input['couriers'][i], dict) == True:
                    try:
                        if (str(data_input['couriers'][i]['id']).isdigit() == True) and (data_input['couriers'][i]['type'] in values_of_type) and (str(data_input['couriers'][i]['district']).isdigit() ==
                            True) and (validator_of_time(data_input['couriers'][i]['time'][:5]) == True) and (data_input['couriers'][i]['id'] not in ids_from_db) and (validator_of_time(data_input['couriers'][i]['time'][6:]) == True):
                                count += 1
                    except KeyError:
                        ValidationError(f"invalid key in data['couriers'][{i}]")
                else:
                    raise ValidationError('data of some courier must be dict')
        else:
            raise ValidationError('empty data of couriers')
    except KeyError:
        raise ValidationError('invalid key')
    finally:
        if count == len(data_input['couriers']):
            return True
        else:
            return False


def validator_data_of_orders(data_input, ids_from_db):
    count = 0
    try:
        if len(data_input['orders']) != 0:
            for i in range(len(data_input['orders'])):
                if isinstance(data_input['orders'][i], dict) == True:
                    try:
                        if (str(data_input['orders'][i]['id']).isdigit() == True) and (str(data_input['orders'][i]['price']).isnumeric() == True) and (validator_of_time(data_input['orders'][i]['time'][:5])
                            == True) and (validator_of_time(data_input['orders'][i]['time'][6:]) == True) and (str(data_input['orders'][i]['weight']).isnumeric() == True) and (data_input['orders'][i]['id'] not in ids_from_db):
                            count += 1
                    except KeyError:
                        raise ValidationError(f"key invalid in data['orders'][{i}]")
                else:
                    raise ValidationError('data of some order must be dict')
        else:
            raise ValidationError('empty data of orders')
    except KeyError:
        raise ValidationError('invalid key')
    finally:
        if count == len(data_input['orders']):
            return True
        else:
            return False


def validator_of_completed_order(data_input, ids_of_orders_and_couriers_from_db):
    count = 0
    try:
        if len(data_input['orders_completed']) != 0:
            for i in range(len(data_input['orders_completed'])):
                for j in range(len(ids_of_orders_and_couriers_from_db)):
                    try:
                        if (ids_of_orders_and_couriers_from_db[j]['id_order'] == data_input['orders_completed'][i]['id_order']) and (ids_of_orders_and_couriers_from_db[j]['id_courier'] == data_input['orders_completed'][i]['id_courier']) and \
                                (validator_of_time(data_input['orders_completed'][i]['time'][11:]) == True) and (validator_of_data(data_input['orders_completed'][i]['time'][:10]) == True):
                            count += 1
                    except KeyError:
                        raise ValidationError(f"invalid key in data['orders_completed'][{i}]")
        else:
            raise ValidationError('empty data of orders_completed')
    except KeyError:
        return ValidationError('invalid key')
    finally:
        if count == len(data_input['orders_completed']):
            return True
        else:
            return False


def validator_data_for_rating(data_input):
    try:
        if len(data_input['interval']) != 0:
            try:
                if validator_of_data(data_input['interval'][0]['end_date']) == True and validator_of_data(data_input['interval'][0]['start_date']) == True:
                    return True
                else:
                    try:
                        if len(data_input['interval'][0]['end_date']) == 10 and len(data_input['interval'][0]['start_date']) > 10:
                            if validator_of_data(data_input['interval'][0]['end_date']) == True and validator_of_data(data_input['interval'][0]['start_date'][:10]) == True and validator_of_time(data_input['interval'][0]['start_date'][11:]) == True:
                                return True
                        if len(data_input['interval'][0]['end_date']) > 10 and len(data_input['interval'][0]['start_date']) == 10:
                            if validator_of_data(data_input['interval'][0]['start_date']) == True and validator_of_data(data_input['interval'][0]['end_date'][:10]) == True and validator_of_time(data_input['interval'][0]['end_date'][11:]) == True:
                                return True
                        if len(data_input['interval'][0]['end_date']) == 10 and len(data_input['interval'][0]['start_date']) == 10:
                            if validator_of_data(data_input['interval'][0]['start_date']) == True and validator_of_data(data_input['interval'][0]['end_date']) == True:
                                return True
                        if validator_of_data(data_input['interval'][0]['end_date'][:10]) == True and validator_of_data(data_input['interval'][0]['start_date'][:10]) == True and\
                            validator_of_time(data_input['interval'][0]['end_date'][11:]) == True and validator_of_time(data_input['interval'][0]['start_date'][11:]) == True:
                            return True
                        else:
                            return False
                    except:
                        ValidationError('data invalid')
            except KeyError:
                ValidationError('key invalid')
        else:
            raise ValidationError('empty data of date')
    except KeyError:
        return ValidationError('key invalid')


slovar_time_of_requests = {}
def limiter(str1):
    if str1 in slovar_time_of_requests:
        time_of_request = datetime.datetime.now()
        slovar_time_of_requests[f'{str1}'].sort()
        values = []
        for i in range(len(slovar_time_of_requests[f'{str1}'])-1):
            if (time_of_request - slovar_time_of_requests[f'{str1}'][i]).seconds > 0:
                values.append(slovar_time_of_requests[f'{str1}'][i])
        for j in range(len(values)):
            slovar_time_of_requests[f'{str1}'].remove(values[j])
        if len(slovar_time_of_requests[f'{str1}']) < 10:
            slovar_time_of_requests[f'{str1}'].append(time_of_request)
        else:
            return False
    else:
        slovar_time_of_requests[f'{str1}'] = [datetime.datetime.now()]
    return True
