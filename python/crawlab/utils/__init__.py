import os

from crawlab.constants import DedupMethod


def get_task_id():
    return os.environ.get('CRAWLAB_TASK_ID')


def get_is_dedup():
    return os.environ.get('CRAWLAB_IS_DEDUP')


def get_dedup_field():
    return os.environ.get('CRAWLAB_DEDUP_FIELD')


def get_dedup_method():
    return os.environ.get('CRAWLAB_DEDUP_METHOD')


def save_item(item):
    try:
        from crawlab.db import col

        # 赋值task_id
        item['task_id'] = get_task_id()

        # 是否开启去重
        is_dedup = get_is_dedup()

        if is_dedup == '1':
            # 去重
            dedup_field = get_dedup_field()
            dedup_method = get_dedup_method()
            if dedup_method == DedupMethod.OVERWRITE:
                # 覆盖
                col.remove({dedup_field: item[dedup_field]})
                col.save(item)
            elif dedup_method == DedupMethod.IGNORE:
                # 忽略
                col.save(item)
            else:
                # 其他
                col.save(item)
        else:
            # 不去重
            col.save(item)
    except:
        pass

def save_item_2pg(item, sess, obj, sampling=100, **kw):
    '''
    Provide map2pg=True to redirect to postgres and sampling result to mongo
    Need to provide additionally params:
        sess = (pgsession)
        item = (ORM instance)
        obj = (ORM object)
        sampling = (samply rate)  ## 100 for 1/100 of result to store in mongo, 0 for NO SAMPLING
    '''
    from crawlab.utils.toolbox import storeObject
    from sqlalchemy.orm.session import Session
    import random
    if not (obj.__class__.__name__ == 'DeclarativeMeta' and int(sampling) >= 0 and \
        isinstance(item, obj) and isinstance(sess, Session)):     ## Check params are valid type
        return None
    if not get_task_id():               ## To ensure func are called from valid node
        return None
    try:
        if storeObject(obj, item, sess) == True:
            if not sampling == 0:
                if random.randint(1, int(sampling)) == 1:
                    item_dict = {key:val for key,val in item.__dict__.items() if key != '_sa_instance_state'}
                    save_item(item_dict)
            return True
        else:
            return False
    except Exception as e:
        print(e)
        return False