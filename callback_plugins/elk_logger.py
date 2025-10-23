# Make coding more python3-ish
from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = '''
name: elasticsearch
callback_type: notification
requirements:
    - requests (pip install requests)
short_description: Send Ansible logs to Elasticsearch
version_added: "2.0"
description:
    - This callback plugin sends Ansible playbook execution logs to Elasticsearch/Kibana
options:
  elasticsearch_host:
    description: Elasticsearch host URL
    default: "http://localhost:9200"
    env:
      - name: ANSIBLE_ELASTICSEARCH_HOST
    ini:
      - section: callback_elasticsearch
        key: host
  elasticsearch_index:
    description: Elasticsearch index name
    default: "ansible-logs"
    env:
      - name: ANSIBLE_ELASTICSEARCH_INDEX
    ini:
      - section: callback_elasticsearch
        key: index
  log_task_results:
    description: Log detailed task results
    type: bool
    default: True
    env:
      - name: ANSIBLE_ELASTICSEARCH_LOG_RESULTS
    ini:
      - section: callback_elasticsearch
        key: log_task_results
'''

from datetime import datetime
from ansible.plugins.callback import CallbackBase
import json
import socket

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


class CallbackModule(CallbackBase):
    """
    This callback module sends Ansible logs to Elasticsearch.
    """
    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'notification'
    CALLBACK_NAME = 'elk_logger'
    CALLBACK_NEEDS_ENABLED = True

    def __init__(self):
        super(CallbackModule, self).__init__()
        
        if not HAS_REQUESTS:
            self._display.warning('requests library is required for elasticsearch callback')
            self.disabled = True
            return
        
        self.playbook_name = None
        self.playbook_id = None
        self.start_time = None
        self.hostname = socket.gethostname()

    def set_options(self, task_keys=None, var_options=None, direct=None):
        super(CallbackModule, self).set_options(task_keys=task_keys, var_options=var_options, direct=direct)
        
        self.es_host = self.get_option('elasticsearch_host')
        self.es_index = self.get_option('elasticsearch_index')
        self.log_task_results = self.get_option('log_task_results')

    def v2_playbook_on_start(self, playbook):
        """Appelé au démarrage du playbook"""
        self.playbook_name = playbook._file_name
        self.start_time = datetime.utcnow()
        self.playbook_id = f"{self.hostname}-{self.start_time.strftime('%Y%m%d%H%M%S')}"
        
        log_entry = {
            "@timestamp": self.start_time.isoformat() + "Z",
            "event_type": "playbook_start",
            "playbook_id": self.playbook_id,
            "playbook": self.playbook_name,
            "hostname": self.hostname,
            "status": "started"
        }
        
        self._send_to_elasticsearch(log_entry)
        self._display.display(f"[Elasticsearch] Playbook started: {self.playbook_name}")

    def v2_playbook_on_play_start(self, play):
        """Appelé au démarrage de chaque play"""
        log_entry = {
            "@timestamp": datetime.utcnow().isoformat() + "Z",
            "event_type": "play_start",
            "playbook_id": self.playbook_id,
            "playbook": self.playbook_name,
            "play": play.get_name(),
            "hostname": self.hostname
        }
        
        self._send_to_elasticsearch(log_entry)

    def v2_playbook_on_task_start(self, task, is_conditional):
        """Appelé au démarrage de chaque task"""
        log_entry = {
            "@timestamp": datetime.utcnow().isoformat() + "Z",
            "event_type": "task_start",
            "playbook_id": self.playbook_id,
            "playbook": self.playbook_name,
            "task": task.get_name(),
            "hostname": self.hostname
        }
        
        self._send_to_elasticsearch(log_entry)

    def v2_runner_on_ok(self, result):
        """Appelé quand une task réussit"""
        log_entry = {
            "@timestamp": datetime.utcnow().isoformat() + "Z",
            "event_type": "task_ok",
            "playbook_id": self.playbook_id,
            "playbook": self.playbook_name,
            "host": result._host.get_name(),
            "task": result._task.get_name(),
            "status": "success",
            "changed": result._result.get('changed', False),
            "hostname": self.hostname
        }
        
        if self.log_task_results:
            log_entry['result'] = self._clean_result(result._result)
        
        self._send_to_elasticsearch(log_entry)

    def v2_runner_on_failed(self, result, ignore_errors=False):
        """Appelé quand une task échoue"""
        log_entry = {
            "@timestamp": datetime.utcnow().isoformat() + "Z",
            "event_type": "task_failed",
            "playbook_id": self.playbook_id,
            "playbook": self.playbook_name,
            "host": result._host.get_name(),
            "task": result._task.get_name(),
            "status": "failed",
            "ignore_errors": ignore_errors,
            "hostname": self.hostname,
            "error_message": result._result.get('msg', 'Unknown error')
        }
        
        if self.log_task_results:
            log_entry['result'] = self._clean_result(result._result)
        
        self._send_to_elasticsearch(log_entry)
        self._display.warning(f"[Elasticsearch] Task failed: {result._task.get_name()}")

    def v2_runner_on_skipped(self, result):
        """Appelé quand une task est skippée"""
        log_entry = {
            "@timestamp": datetime.utcnow().isoformat() + "Z",
            "event_type": "task_skipped",
            "playbook_id": self.playbook_id,
            "playbook": self.playbook_name,
            "host": result._host.get_name(),
            "task": result._task.get_name(),
            "status": "skipped",
            "hostname": self.hostname
        }
        
        self._send_to_elasticsearch(log_entry)

    def v2_runner_on_unreachable(self, result):
        """Appelé quand un host est unreachable"""
        log_entry = {
            "@timestamp": datetime.utcnow().isoformat() + "Z",
            "event_type": "host_unreachable",
            "playbook_id": self.playbook_id,
            "playbook": self.playbook_name,
            "host": result._host.get_name(),
            "task": result._task.get_name(),
            "status": "unreachable",
            "hostname": self.hostname,
            "error_message": result._result.get('msg', 'Host unreachable')
        }
        
        self._send_to_elasticsearch(log_entry)
        self._display.error(f"[Elasticsearch] Host unreachable: {result._host.get_name()}")

    def v2_playbook_on_stats(self, stats):
        """Appelé à la fin du playbook avec les statistiques"""
        end_time = datetime.utcnow()
        runtime = end_time - self.start_time
        
        hosts = sorted(stats.processed.keys())
        summary = {}
        
        for h in hosts:
            s = stats.summarize(h)
            summary[h] = {
                "ok": s['ok'],
                "changed": s['changed'],
                "unreachable": s['unreachable'],
                "failures": s['failures'],
                "skipped": s['skipped'],
                "rescued": s['rescued'],
                "ignored": s['ignored']
            }
        
        log_entry = {
            "@timestamp": end_time.isoformat() + "Z",
            "event_type": "playbook_stats",
            "playbook_id": self.playbook_id,
            "playbook": self.playbook_name,
            "status": "completed",
            "start_time": self.start_time.isoformat() + "Z",
            "end_time": end_time.isoformat() + "Z",
            "duration_seconds": runtime.total_seconds(),
            "hostname": self.hostname,
            "summary": summary
        }
        
        self._send_to_elasticsearch(log_entry)
        self._display.display(f"[Elasticsearch] Playbook completed in {runtime.total_seconds():.2f}s")

    def _clean_result(self, result):
        """Nettoie et limite la taille des résultats"""
        clean = {}
        safe_keys = ['stdout', 'stderr', 'msg', 'changed', 'rc', 'stdout_lines']
        
        for key in safe_keys:
            if key in result:
                value = result[key]
                # Limite la taille des strings pour éviter les documents trop gros
                if isinstance(value, str) and len(value) > 5000:
                    clean[key] = value[:5000] + "... (truncated)"
                elif isinstance(value, list) and len(value) > 50:
                    clean[key] = value[:50] + ["... (truncated)"]
                else:
                    clean[key] = value
        
        return clean

    def _send_to_elasticsearch(self, log_entry):
        """Envoie les logs vers Elasticsearch"""
        if self.disabled:
            return
        
        try:
            url = f"{self.es_host}/{self.es_index}/_doc"
            headers = {"Content-Type": "application/json"}
            
            response = requests.post(
                url,
                data=json.dumps(log_entry),
                headers=headers,
                timeout=5
            )
            
            if response.status_code not in [200, 201]:
                self._display.warning(
                    f"[Elasticsearch] Failed to send log: HTTP {response.status_code}"
                )
                
        except requests.exceptions.RequestException as e:
            self._display.warning(f"[Elasticsearch] Error: {str(e)}")