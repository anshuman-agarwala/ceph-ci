# Default container images -----------------------------------------------------

from typing import NamedTuple
from enum import Enum


class ContainerImage(NamedTuple):
    image_ref: str  # reference to default container image
    key: str  # image key
    desc: str  # description of image

    def __repr__(self) -> str:
        return self.image_ref


def create_image(image_ref: str, key: str) -> ContainerImage:
    _img_prefix = 'container_image_'
    description = key.replace('_', ' ').capitalize()
    return ContainerImage(
        image_ref,
        f'{_img_prefix}{key}',
        f'{description} container image'
    )


class DefaultImages(Enum):
    PROMETHEUS = create_image('quay.io/prometheus/prometheus:v2.51.0', 'prometheus')
    LOKI = create_image('docker.io/grafana/loki:3.0.0', 'loki')
    PROMTAIL = create_image('docker.io/grafana/promtail:3.0.0', 'promtail')
    NODE_EXPORTER = create_image('quay.io/prometheus/node-exporter:v1.7.0', 'node_exporter')
    ALERTMANAGER = create_image('quay.io/prometheus/alertmanager:v0.27.0', 'alertmanager')
    GRAFANA = create_image('quay.io/ceph/grafana:10.4.8', 'grafana')
    HAPROXY = create_image('quay.io/ceph/haproxy:2.3', 'haproxy')
    KEEPALIVED = create_image('quay.io/ceph/keepalived:2.2.4', 'keepalived')
    NVMEOF = create_image('quay.io/ceph/nvmeof:1.2.17', 'nvmeof')
    SNMP_GATEWAY = create_image('docker.io/maxwo/snmp-notifier:v1.2.1', 'snmp_gateway')
    ELASTICSEARCH = create_image('quay.io/omrizeneva/elasticsearch:6.8.23', 'elasticsearch')
    JAEGER_COLLECTOR = create_image('quay.io/jaegertracing/jaeger-collector:1.29',
                                    'jaeger_collector')
    JAEGER_AGENT = create_image('quay.io/jaegertracing/jaeger-agent:1.29', 'jaeger_agent')
    JAEGER_QUERY = create_image('quay.io/jaegertracing/jaeger-query:1.29', 'jaeger_query')
    SAMBA = create_image('quay.io/samba.org/samba-server:devbuilds-centos-amd64', 'samba')
    SAMBA_METRICS = create_image('quay.io/samba.org/samba-metrics:latest', 'samba_metrics')
    NGINX = create_image('quay.io/ceph/nginx:sclorg-nginx-126', 'nginx')
    OAUTH2_PROXY = create_image('quay.io/oauth2-proxy/oauth2-proxy:v7.6.0', 'oauth2_proxy')

    @property
    def image_ref(self) -> str:
        return self.value.image_ref

    @property
    def key(self) -> str:
        return self.value.key

    @property
    def desc(self) -> str:
        return self.value.desc
