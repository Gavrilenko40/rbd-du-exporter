#!/usr/bin/python3

from prometheus_client import start_http_server, Summary, Gauge
import random
import time
import rados
import rbd

class DiffCounter:
    def __init__(self):
        self.count = 0
    def cb_offset(self, offset, length, exists):
        if exists:
            self.count+=4194304


def disk_usage():
    global provisioned_size
    global used_size
    cluster = rados.Rados(conffile='/etc/ceph/ceph.conf')
    cluster.connect()
    try:
        ioctx = cluster.open_ioctx('nvme')
        try:
            rbd_inst = rbd.RBD()
            try:
                for image_name in rbd_inst.list(ioctx):
                    try:
                        image = rbd.Image(ioctx, image_name, read_only=True)
                    except Exception as e:
                        print(e)
                    max_size = image.size()
                    counter = DiffCounter()
                    image.diff_iterate(0,max_size,None,counter.cb_offset)
                    current_size = counter.count
                    print(image_name,max_size,current_size)
                    provisioned_size.labels('nvme', image_name).set(max_size)
                    used_size.labels('nvme', image_name).set(current_size)
            except:
                pass
            finally:
                image.close()
        finally:
            ioctx.close()
    finally:
        cluster.shutdown()

if __name__ == '__main__':
    provisioned_size = Gauge('rbd_provisioned_size', 'Rbd provisioned size in bytes', ['pool', 'image'])
    used_size = Gauge('rbd_used_size', 'Rbd used size in bytes', ['pool', 'image'])

    # Start up the server to expose the metrics.
    start_http_server(8123)
    
    # Generate some requests.
    while True:
        disk_usage()
        time.sleep(300)
        #process_request() #random.random())
