# Default container images -----------------------------------------------------

from typing import NamedTuple
from enum import Enum


class ContainerImage(NamedTuple):
    name: str
    key: str
    desc: str


_img_prefix = 'container_image_'


class DefaultImages(Enum):
    PROMETHEUS = ContainerImage(
        'quay.io/prometheus/prometheus:v2.51.0',
        f'{_img_prefix}prometheus',
        'Prometheus container image'
    )
    LOKI = ContainerImage(
        'docker.io/grafana/loki:3.0.0',
        f'{_img_prefix}loki',
        'Loki container image'
    )
    PROMTAIL = ContainerImage(
        'docker.io/grafana/promtail:3.0.0',
        f'{_img_prefix}promtail',
        'Promtail container image'
    )
    NODE_EXPORTER = ContainerImage(
        'quay.io/prometheus/node-exporter:v1.7.0',
        f'{_img_prefix}node_exporter',
        'Node exporter container image'
    )
    ALERTMANAGER = ContainerImage(
        'quay.io/prometheus/alertmanager:v0.27.0',
        f'{_img_prefix}alertmanager',
        'Alertmanager container image'
    )
    GRAFANA = ContainerImage(
        'quay.io/ceph/grafana:10.4.8',
        f'{_img_prefix}grafana',
        'Grafana container image'
    )
    HAPROXY = ContainerImage(
        'quay.io/ceph/haproxy:2.3',
        f'{_img_prefix}haproxy',
        'HAproxy container image'
    )
    KEEPALIVED = ContainerImage(
        'quay.io/ceph/keepalived:2.2.4',
        f'{_img_prefix}keepalived',
        'Keepalived container image'
    )
    NVMEOF = ContainerImage(
        'quay.io/ceph/nvmeof:1.2.17',
        f'{_img_prefix}nvmeof',
        'Nvme-of container image'
    )
    SNMP_GATEWAY = ContainerImage(
        'docker.io/maxwo/snmp-notifier:v1.2.1',
        f'{_img_prefix}snmp_gateway',
        'SNMP Gateway container image'
    )
    ELASTICSEARCH = ContainerImage(
        'quay.io/omrizeneva/elasticsearch:6.8.23',
        f'{_img_prefix}elasticsearch',
        'elasticsearch container image'
    )
    JAEGER_COLLECTOR = ContainerImage(
        'quay.io/jaegertracing/jaeger-collector:1.29',
        f'{_img_prefix}jaeger_collector',
        'Jaeger collector container image'
    )
    JAEGER_AGENT = ContainerImage(
        'quay.io/jaegertracing/jaeger-agent:1.29',
        f'{_img_prefix}jaeger_agent',
        'Jaeger agent container image'
    )
    JAEGER_QUERY = ContainerImage(
        'quay.io/jaegertracing/jaeger-query:1.29',
        f'{_img_prefix}jaeger_query',
        'Jaeger query container image'
    )
    SAMBA = ContainerImage(
        'quay.io/samba.org/samba-server:devbuilds-centos-amd64',
        f'{_img_prefix}samba',
        'Samba/SMB container image'
    )
    SAMBA_METRICS = ContainerImage(
        'quay.io/samba.org/samba-metrics:latest',
        f'{_img_prefix}samba_metrics',
        'Samba/SMB metrics exporter container image'
    )
    NGINX = ContainerImage(
        'quay.io/ceph/nginx:sclorg-nginx-126',
        f'{_img_prefix}nginx',
        'Nginx container image'
    )
    OAUTH2_PROXY = ContainerImage(
        'quay.io/oauth2-proxy/oauth2-proxy:v7.6.0',
        f'{_img_prefix}oauth2_proxy',
        'oauth2-proxy container image'
    )
