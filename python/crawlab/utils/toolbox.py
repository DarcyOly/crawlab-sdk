import traceback

def storeObject(obj, data, sess):
    target = obj()
    keys = list(data.__dict__.keys())
    try:
        keys.remove('_sa_instance_state')
    except:
        pass
    for key in keys:
        target.__setattr__(key, data.__getattribute__(key))
    try:
        sess.add(target)
        sess.commit()
        return True
    except Exception as e:
        traceback.print_exc()
        print(e)
        sess.rollback()
        return False
    finally:
        sess.close()        ## return Session to pool