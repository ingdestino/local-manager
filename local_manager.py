from ast import Not
import os
import sys
import json
import time
import logging
import threading
import tornado.httpclient
from tornado.ioloop import IOLoop
from proton.reactor import Container, EventInjector, ApplicationEvent
from proton.handlers import MessagingHandler
from tornado.web import Application, RequestHandler
from queue import Queue


##############################################################################
##############################################################################
##############################################################################

LOGGER_NAME = 'LocalManagerLogger'
TIME_LOG_NAME = ' Timing Local Manager '
OUT_OF_COVERAGE_SM_KEY = "default"

CONN_RETRY_TIMEOUT_MIN = 1
CONN_RETRY_TIMEOUT_MAX = 8


LOG_LEVEL = 'LOG_LEVEL'
SM_RESEND_TIMEOUT = "SUPPORT_MESSAGE_RESEND_TIMEOUT"
API_PORT = "API_PORT"
RESPONSE_ROUTER_EP = "RESPONSE_ROUTER_EP"
RESPONSE_ROUTER_API = "RESPONSE_ROUTER_API"
RECEIVER_BROKER_TOPIC = "RECEIVER_BROKER_TOPIC"

OSENV_LOG_LEVEL = os.environ[LOG_LEVEL]
OSENV_SM_RESEND_TIMEOUT = float(os.environ[SM_RESEND_TIMEOUT])
OSENV_API_PORT = int(os.environ[API_PORT])
OSENV_RESPONSE_ROUTER_EP = os.environ[RESPONSE_ROUTER_EP]
OSENV_RESPONSE_ROUTER_API = os.environ[RESPONSE_ROUTER_API]
OSENV_RECEIVER_BROKER_TOPIC = os.environ[RECEIVER_BROKER_TOPIC]

##############################################################################
# LOGGER  ####################################################################
##############################################################################

formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')


def logger_setup(name, level=OSENV_LOG_LEVEL):
    """Setup different loggers here"""

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(sh)
    logger.propagate = False

    return logger


def logger_file_setup(name, file_name, level=OSENV_LOG_LEVEL):
    """Setup different file-loggers here"""

    file_handler = logging.FileHandler(file_name)
    file_handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(file_handler)

    return logger


general_log = logger_setup(LOGGER_NAME)
time_log = logger_setup(TIME_LOG_NAME)


##############################################################################
# TREACE THREAD ##############################################################
##############################################################################


class TraceThread(threading.Thread):
    """ Simple thread class to kill threads using traces"""

    def __init__(self, *args, **keywords):
        threading.Thread.__init__(self, *args, **keywords)
        self.killed = False

    def start(self):
        self.__run_backup = self.run
        self.run = self.__run
        threading.Thread.start(self)

    def __run(self):
        sys.settrace(self.globaltrace)
        self.__run_backup()
        self.run = self.__run_backup

    def globaltrace(self, frame, why, arg):
        if why == 'call':
            return self.localtrace
        else:
            return None

    def localtrace(self, frame, why, arg):
        if self.killed:
            if why == 'line':
                raise SystemExit()
        return self.localtrace

    def kill(self):
        self.killed = True

##############################################################################
# KILL OLD THREADS ###########################################################
##############################################################################


def kill_old_threads():
    """ Kill older threads in case of config update"""

    for i in threading.enumerate():
        if i is not threading.main_thread() and isinstance(i, TraceThread):
            i.kill()
        else:
            pass


##############################################################################
# DICTS ######################################################################
##############################################################################


"""
    sm_dict: Support Message dict

    sm_dict = {
        <msg_id> : <msg_endpoints>
        ...
    }
"""
sm_dict = {}
"""
    qtcode_dict: Quadtree code dict

    qtcode_dict = {
        <qtcode> : <msg_id>
        ...
    }
"""
qtcode_dict = {}


def get_config(json_config):
    """
    qtcode_dict = {
        <qtcode> : <message_id>
        <qtcode> : <message_id>
        ...
    }

    sm_dict = {
        <message_id>: <message_endpoints>
        ...
    }
    """

    amqp_ep = json_config["ref_amqp_ep"]
    amqp_user = json_config["ref_amqp_user"]
    amqp_password = json_config["ref_amqp_password"]
    for lm in json_config["local_config"]:
        if not lm["qtcode_list"]:
            sm_dict.update(
                {lm["message"].get("id"): lm["message"].get("endpoints")})
        else:
            for qt in lm["qtcode_list"]:
                qtcode_dict.update(
                    {qt: lm["message"].get("id")})
                sm_dict.update(
                    {lm["message"].get("id"): lm["message"].get("endpoints")})
    return sm_dict, qtcode_dict, amqp_ep, amqp_user, amqp_password


def show_current_configuration():
    general_log.info("Support Messages")
    general_log.info(json.dumps(sm_dict, sort_keys=True, indent=4))
    general_log.info("QT Codes")
    general_log.info(json.dumps(qtcode_dict, sort_keys=True, indent=4))

##############################################################################
# GLOBALS ####################################################################
##############################################################################


local_manager = None

##############################################################################
# SUBSCRIBER #################################################################
##############################################################################


class Subscriber(MessagingHandler):

    # Useful example for AMQP msg management with proton
    # https://stackoverflow.com/questions/60857609/qpid-proton-python-not-reconnecting-after-long-job
    # https://github.com/martintuk/qpid-python-mre/blob/master/consumer.py

    def __init__(self, server, user="", password="", receive_topics_names=[]):
        super(Subscriber, self).__init__(auto_accept=False)

        self.server = server
        self.user = user
        self.password = password
        self.receive_topic_names = receive_topics_names
        self.connection = None
        self.connection_error_ongoing = True

        self.injector = EventInjector()

        self.queue = Queue()

        self.receivers = {}

        self.timeout_limit_max = CONN_RETRY_TIMEOUT_MAX
        self.timeout_limit_min = CONN_RETRY_TIMEOUT_MIN
        self.timeout_limit = self.timeout_limit_min

        self.sm_resend_timeout = OSENV_SM_RESEND_TIMEOUT

        # car cache struct
        # cache = {
        #     <car_id>: {
        #         <last_qt>: <last qt value received>
        #         <sm_sending_timestamp> = <timestamp of last sm sent >
        #     },
        #     ...
        # }
        self.car_cache = {}

        self.thread = TraceThread(target=self._process_message, daemon=True)
        self.thread.start()

    def _set_connection(self, container):

        connection = container.connect(
            self.server,
            user=self.user,
            password=self.password
        )

        return connection

    def _set_topics(self, container, connection):

        for topic in self.receive_topic_names:
            self.receivers.update(
                {topic: container.create_receiver(connection,
                                                  'topic://%s' % topic)})

    def _reset_connection_and_topics(self, container):

        connection = self._set_connection(container)

        self._set_topics(container, connection)

        self.connection = connection

    def on_start(self, event):

        self._reset_connection_and_topics(event.container)

    def on_disconnected(self, event):
        """ Triggers the disconected event when the connection is lost"""

        self.connection_error_ongoing = True

        general_log.error("ON_DISCONNECT: The connection to broker is lost. "
                          "Trying to reestablish the connection")

        self._manage_connection_error(event)

        return super().on_disconnected(event)

    def _manage_connection_error(self, event):

        self.connection.close()

        print("Wait for " + str(self.timeout_limit) + " seconds before "
              "resetting the connection\n")

        time.sleep(self.timeout_limit)

        self._reset_connection_and_topics(event.container)

        self.timeout_limit = min(CONN_RETRY_TIMEOUT_MAX,
                                 self.timeout_limit * 2)

        print("Current connection reset timeout set to " +
              str(self.timeout_limit) +
              " seconds\n")

    def get_connection_state(self):
        try:
            state = self.connection.state
            if state == 18:
                self.timeout_limit = self.timeout_limit_min
                if (self.connection_error_ongoing):
                    general_log.info("Connection RESTORED.")
                    general_log.info(
                        "Current connection reset timeout set to " +
                        str(self.timeout_limit) +
                        " seconds\n")
                    self.connection_error_ongoing = False

        except Exception:
            return 0
        return state

    def _process_message(self):
        """
        Consume queue items, process them, and trigger an event for the main
        thread to catch and accept/reject the delivery.
        """
        while True:
            # blocks the thread until there's an item to process
            event, msg_arrival_time = self.queue.get(True)

            general_log.debug("_process_message")

            if self.connection_error_ongoing:
                self.get_connection_state()

            self._process_car_message(event, msg_arrival_time)

            # once the event is processed, trigger a custom application-event.
            self.injector.trigger(
                ApplicationEvent("message_handled",
                                 delivery=event.delivery,
                                 subject=event.message.body))

            # notify the queue that the task has been processed
            self.queue.task_done()

    def _update_car_cache(self, car_id, car_qt, lm_time=None):
        if lm_time is None:
            lm_time = time.time()
        self.car_cache[car_id] = {
                "last_qt": car_qt,
                "sm_sending_timestamp": lm_time
        }

    def _get_time_since_last_sm(self, car_id):
        now = time.time()
        return now - self.car_cache[car_id]["sm_sending_timestamp"]

    def _get_last_position(self, car_id):
        return self.car_cache[car_id]["last_qt"]

    def _shall_send_sm(self, car_id, car_qt):
        if (car_id not in self.car_cache.keys()):
            return True
        else:
            delta_time = self._get_time_since_last_sm(car_id)
            old_car_qt = self._get_last_position(car_id)
            if ((car_qt != old_car_qt) or
               (delta_time > OSENV_SM_RESEND_TIMEOUT)):
                return True
        return False

    def _get_support_message(self, car_id, car_qt, lm_time, car_ref_time):

        message = {
            "id": qtcode_dict[car_qt],
            "endpoints": sm_dict[qtcode_dict[car_qt]]
        }
        return json.dumps({
            'messages': [
                {'Car_ID': car_id,
                    'message': message,
                    'timestamp_lm': lm_time,
                    'ref_timestamp_fc': car_ref_time}]})

    def _get_out_of_coverage_support_message(self, car_id, lm_time,
                                             car_ref_time):
        return json.dumps({
            'messages': [
                {'Car_ID': car_id,
                    'message': sm_dict[OUT_OF_COVERAGE_SM_KEY],
                    'timestamp_lm': lm_time,
                    'ref_timestamp_fc': car_ref_time}]})

    def _process_car_message(self, event, msg_arrival_time):

        # data = None
        car_id = None
        car_ref_time = None
        car_qt = None
        try:
            # data = json.loads(event.message.body)
            car_id = event.message.properties["Car_ID"]  # data["Car_ID"]
            car_ref_time = float(event.message.properties["timestamp"])
            car_qt = event.message.properties["quadTree"]
        except Exception as e:
            general_log.error(e)
            return

        general_log.info("Car "+str(car_id)+" is in "+car_qt)

        sm = None

        if car_qt in qtcode_dict.keys():

            if self._shall_send_sm(car_id, car_qt):
                lm_time = time.time()
                sm = self._get_support_message(car_id, car_qt, lm_time,
                                               car_ref_time)

                self._update_car_cache(car_id, car_qt, lm_time)

        else:
            lm_time = time.time()
            sm = self._get_out_of_coverage_support_message(car_id,
                                                           lm_time,
                                                           car_ref_time)

        if sm is not None:
            self._send_sm(sm)
            # send_change_ep_msg = time.time()
            sm_id = OUT_OF_COVERAGE_SM_KEY
            if car_qt in qtcode_dict:
                sm_id = qtcode_dict[car_qt]
            time_log.debug(
                "Local Manager replied with SM %s sent to car"
                " %s after %sms from CAM arrival" %
                (sm_id,
                 str(car_id),
                 str((time.time() - msg_arrival_time) * 1000)))

    def on_message(self, event):

        general_log.debug("on_message")

        if not self.connection_error_ongoing:

            self.queue.put((event, time.time()))

        else:

            self.get_connection_state()

    def on_message_handled(self, event):
        """
        Capture event triggered from the worker thread in order to ack the
        message accordingly.
        This could be unfolded into accept/reject events if needed, just
        accepting it here.
        """
        self.accept(event.delivery)
        print('job accepted: ' + str(event.subject))

    def _send_sm(self, msg):
        change_ep_of_car(msg)

    def on_connection_error(self, event):
        print('connection_error', event.connection.condition,
              event.connection.remote_condition)

##############################################################################
# REQUEST HANDLERS ###########################################################
##############################################################################


class MMtoLMConfig(RequestHandler):
    """
    API SERVER for handling calls from the main manager regarding LM
    configuration
    """

    def post(self, id):
        global sm_dict
        global qtcode_dict
        global local_manager
        """Handles the behaviour of POST calls"""
        json_form = json.loads(self.request.body)
        sm_dict, qtcode_dict, amqp_ep, amqp_user, amqp_password =\
            get_config(json_form)
        if threading.active_count() > 1:
            kill_old_threads()
        topics = [OSENV_RECEIVER_BROKER_TOPIC]  # ["FROM_CARS"]

        if local_manager is not None:
            del local_manager

        local_manager = Subscriber(amqp_ep, amqp_user, amqp_password, topics)
        container = Container(local_manager)
        qpid_thread_sub = TraceThread(target=container.run)
        qpid_thread_sub.start()

        show_current_configuration()

        time.sleep(1)

        general_log.debug("LM connection with broker %s, topic %s,"
                          " status: %s" % (
                            amqp_ep,
                            topics,
                            local_manager.get_connection_state()))

    def put(self, id):
        """Handles the behaviour of PUT calls"""
        self.write("Not supposed to PUT!")

    def get(self, id):
        """Handles the behaviour of GET calls"""
        self.write('NOT SUPPOSED TO GET!')

    def delete(self, id):
        """Handles the behaviour of DELETE calls"""
        self.write("Not supposed to DELETE!")


##############################################################################
# MAIN STUFF #################################################################
##############################################################################


def change_ep_of_car(bjson):
    """ To change the endpoint of the car in case of chaging zones """
    # start_change_ep = time.time()

    url = "http://" + OSENV_RESPONSE_ROUTER_EP + OSENV_RESPONSE_ROUTER_API

    try:
        response = http_client.fetch(
            url,
            method='POST',
            body=bjson)
    except Exception as e:
        general_log.error("Error: %s" % e)
        general_log.error("Couldn't POST to Response Router")
    else:
        general_log.debug(response.body)
        # general_log.debug( str((time.time()-start_change_ep)*1000)+
        #                    " ms to POST")


def make_app():
    urls = [
        (r"/api/item/from_main_mgr_config_api/([^/]+)?", MMtoLMConfig)
    ]
    return Application(urls, debug=True)


if __name__ == '__main__':

    app = make_app()
    app.listen(OSENV_API_PORT)
    general_log.debug("Started Local Manager REST Server, "
                      "listening at %s" % OSENV_API_PORT)
    http_client = tornado.httpclient.HTTPClient()
    IOLoop.instance().start()
