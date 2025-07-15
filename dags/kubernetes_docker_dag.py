"""
Kubernetes and Docker DAG Example
==================================

This DAG demonstrates containerized task execution patterns commonly asked about in interviews:
- DockerOperator usage and configuration
- KubernetesPodOperator for container orchestration
- Resource management in containerized environments
- Volume mounts and environment variables
- Container lifecycle management
- Secrets and ConfigMaps usage

Note: This DAG requires proper Docker and Kubernetes configuration.
For local testing, some tasks are simulated.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import json
import os

def simulate_docker_task(**context):
    """Simulate Docker container execution."""
    print("=== Docker Container Simulation ===")
    
    # Simulate Docker container configuration
    docker_config = {
        'image': 'python:3.9-slim',
        'command': ['python', '-c', 'print("Hello from Docker container!")'],
        'environment': {
            'PYTHONPATH': '/app',
            'ENV': 'production',
            'LOG_LEVEL': 'INFO'
        },
        'volumes': [
            '/tmp/airflow_data:/app/data:ro',
            '/tmp/airflow_logs:/app/logs:rw'
        ],
        'working_dir': '/app',
        'user': '1000:1000'
    }
    
    print("Docker container configuration:")
    print(json.dumps(docker_config, indent=2))
    
    # Simulate container execution
    print("\nContainer execution steps:")
    print("1. Pulling Docker image...")
    print("2. Creating container with specified configuration...")
    print("3. Starting container...")
    print("4. Executing command: python -c 'print(\"Hello from Docker container!\")'")
    print("5. Container output: Hello from Docker container!")
    print("6. Container completed with exit code: 0")
    print("7. Cleaning up container...")
    
    return {
        'container_id': 'container_123abc',
        'exit_code': 0,
        'execution_time': 45.2,
        'memory_usage': '128MB'
    }

def simulate_kubernetes_pod(**context):
    """Simulate Kubernetes Pod execution."""
    print("=== Kubernetes Pod Simulation ===")
    
    # Simulate Kubernetes Pod specification
    pod_spec = {
        'apiVersion': 'v1',
        'kind': 'Pod',
        'metadata': {
            'name': 'airflow-task-pod',
            'namespace': 'airflow',
            'labels': {
                'app': 'airflow-task',
                'version': 'v1.0'
            }
        },
        'spec': {
            'containers': [{
                'name': 'task-container',
                'image': 'python:3.9-slim',
                'command': ['python', '-c'],
                'args': ['import time; print("Processing..."); time.sleep(5); print("Complete!")'],
                'resources': {
                    'requests': {'memory': '256Mi', 'cpu': '100m'},
                    'limits': {'memory': '512Mi', 'cpu': '500m'}
                },
                'env': [
                    {'name': 'TASK_ID', 'value': 'kubernetes_pod_task'},
                    {'name': 'EXECUTION_DATE', 'value': context['ds']}
                ],
                'volumeMounts': [
                    {'name': 'data-volume', 'mountPath': '/app/data'},
                    {'name': 'config-volume', 'mountPath': '/app/config'}
                ]
            }],
            'volumes': [
                {'name': 'data-volume', 'persistentVolumeClaim': {'claimName': 'data-pvc'}},
                {'name': 'config-volume', 'configMap': {'name': 'app-config'}}
            ],
            'restartPolicy': 'Never',
            'serviceAccountName': 'airflow-task-sa'
        }
    }
    
    print("Kubernetes Pod specification:")
    print(json.dumps(pod_spec, indent=2))
    
    # Simulate pod lifecycle
    print("\nPod execution lifecycle:")
    print("1. Submitting Pod to Kubernetes API...")
    print("2. Scheduler assigning Pod to Node...")
    print("3. Kubelet pulling container image...")
    print("4. Creating and starting container...")
    print("5. Pod Status: Running")
    print("6. Container output: Processing...")
    print("7. Container output: Complete!")
    print("8. Pod Status: Succeeded")
    print("9. Pod cleanup completed")
    
    return {
        'pod_name': 'airflow-task-pod',
        'namespace': 'airflow',
        'status': 'Succeeded',
        'execution_time': 67.5,
        'node': 'worker-node-1'
    }

def demonstrate_container_networking(**context):
    """Demonstrate container networking concepts."""
    print("=== Container Networking ===")
    
    networking_concepts = {
        'bridge_network': {
            'description': 'Default Docker network for container communication',
            'use_case': 'Inter-container communication on single host'
        },
        'host_network': {
            'description': 'Container uses host network stack',
            'use_case': 'High-performance networking requirements'
        },
        'overlay_network': {
            'description': 'Multi-host container networking',
            'use_case': 'Distributed applications across multiple hosts'
        },
        'service_mesh': {
            'description': 'Kubernetes service discovery and load balancing',
            'use_case': 'Microservices communication and traffic management'
        }
    }
    
    print("Container networking patterns:")
    for network_type, details in networking_concepts.items():
        print(f"\n{network_type.upper()}:")
        print(f"  Description: {details['description']}")
        print(f"  Use case: {details['use_case']}")
    
    # Simulate network configuration
    network_config = {
        'network_mode': 'bridge',
        'ports': {'80/tcp': 8080, '443/tcp': 8443},
        'dns': ['8.8.8.8', '8.8.4.4'],
        'hostname': 'airflow-task-container'
    }
    
    print(f"\nNetwork configuration: {json.dumps(network_config, indent=2)}")
    
    return networking_concepts

def demonstrate_container_security(**context):
    """Demonstrate container security best practices."""
    print("=== Container Security ===")
    
    security_practices = {
        'non_root_user': {
            'description': 'Run containers as non-root user',
            'implementation': 'USER 1000:1000 in Dockerfile'
        },
        'read_only_filesystem': {
            'description': 'Mount filesystem as read-only',
            'implementation': 'read_only: true in container spec'
        },
        'resource_limits': {
            'description': 'Set CPU and memory limits',
            'implementation': 'resources.limits in Kubernetes'
        },
        'secrets_management': {
            'description': 'Use Kubernetes Secrets for sensitive data',
            'implementation': 'secretKeyRef in environment variables'
        },
        'network_policies': {
            'description': 'Control pod-to-pod communication',
            'implementation': 'NetworkPolicy resources'
        },
        'security_context': {
            'description': 'Security settings for pods/containers',
            'implementation': 'securityContext in pod spec'
        }
    }
    
    print("Container security best practices:")
    for practice, details in security_practices.items():
        print(f"\n{practice.upper()}:")
        print(f"  Description: {details['description']}")
        print(f"  Implementation: {details['implementation']}")
    
    # Simulate security configuration
    security_config = {
        'securityContext': {
            'runAsNonRoot': True,
            'runAsUser': 1000,
            'runAsGroup': 1000,
            'fsGroup': 2000,
            'capabilities': {
                'drop': ['ALL'],
                'add': ['NET_BIND_SERVICE']
            }
        },
        'readOnlyRootFilesystem': True,
        'allowPrivilegeEscalation': False
    }
    
    print(f"\nSecurity configuration: {json.dumps(security_config, indent=2)}")
    
    return security_practices

def demonstrate_volume_management(**context):
    """Demonstrate volume and storage management."""
    print("=== Volume Management ===")
    
    volume_types = {
        'emptyDir': {
            'description': 'Temporary storage tied to pod lifecycle',
            'use_case': 'Temporary files, caching'
        },
        'hostPath': {
            'description': 'Mount host filesystem path',
            'use_case': 'Access host files, development'
        },
        'persistentVolumeClaim': {
            'description': 'Persistent storage independent of pod lifecycle',
            'use_case': 'Database storage, shared data'
        },
        'configMap': {
            'description': 'Configuration data as volume',
            'use_case': 'Application configuration files'
        },
        'secret': {
            'description': 'Sensitive data as volume',
            'use_case': 'Certificates, API keys'
        }
    }
    
    print("Kubernetes volume types:")
    for volume_type, details in volume_types.items():
        print(f"\n{volume_type.upper()}:")
        print(f"  Description: {details['description']}")
        print(f"  Use case: {details['use_case']}")
    
    # Simulate volume configuration
    volume_config = {
        'volumes': [
            {
                'name': 'data-storage',
                'persistentVolumeClaim': {'claimName': 'data-pvc'}
            },
            {
                'name': 'app-config',
                'configMap': {'name': 'app-config'}
            },
            {
                'name': 'app-secrets',
                'secret': {'secretName': 'app-secrets'}
            }
        ],
        'volumeMounts': [
            {'name': 'data-storage', 'mountPath': '/app/data'},
            {'name': 'app-config', 'mountPath': '/app/config'},
            {'name': 'app-secrets', 'mountPath': '/app/secrets', 'readOnly': True}
        ]
    }
    
    print(f"\nVolume configuration: {json.dumps(volume_config, indent=2)}")
    
    return volume_types

def demonstrate_container_monitoring(**context):
    """Demonstrate container monitoring and observability."""
    print("=== Container Monitoring ===")
    
    monitoring_aspects = {
        'resource_usage': {
            'metrics': ['CPU usage', 'Memory usage', 'Disk I/O', 'Network I/O'],
            'tools': ['cAdvisor', 'Prometheus', 'Grafana']
        },
        'application_metrics': {
            'metrics': ['Request rate', 'Error rate', 'Response time', 'Throughput'],
            'tools': ['Application metrics', 'Custom exporters']
        },
        'logs': {
            'metrics': ['Application logs', 'Container logs', 'System logs'],
            'tools': ['Fluentd', 'Elasticsearch', 'Kibana']
        },
        'health_checks': {
            'metrics': ['Liveness probe', 'Readiness probe', 'Startup probe'],
            'tools': ['Kubernetes probes', 'Health endpoints']
        }
    }
    
    print("Container monitoring aspects:")
    for aspect, details in monitoring_aspects.items():
        print(f"\n{aspect.upper()}:")
        print(f"  Metrics: {', '.join(details['metrics'])}")
        print(f"  Tools: {', '.join(details['tools'])}")
    
    # Simulate monitoring configuration
    monitoring_config = {
        'livenessProbe': {
            'httpGet': {'path': '/health', 'port': 8080},
            'initialDelaySeconds': 30,
            'periodSeconds': 10
        },
        'readinessProbe': {
            'httpGet': {'path': '/ready', 'port': 8080},
            'initialDelaySeconds': 5,
            'periodSeconds': 5
        },
        'resources': {
            'requests': {'memory': '256Mi', 'cpu': '100m'},
            'limits': {'memory': '512Mi', 'cpu': '500m'}
        }
    }
    
    print(f"\nMonitoring configuration: {json.dumps(monitoring_config, indent=2)}")
    
    return monitoring_aspects

def create_container_summary(**context):
    """Create summary of container concepts."""
    print("=== Container Summary ===")
    
    summary = {
        'key_concepts': [
            'Container images and registries',
            'Resource management and limits',
            'Networking and service discovery',
            'Volume management and persistence',
            'Security and access control',
            'Monitoring and observability'
        ],
        'docker_vs_kubernetes': {
            'Docker': 'Container runtime and image management',
            'Kubernetes': 'Container orchestration and management platform'
        },
        'production_considerations': [
            'Image security scanning',
            'Resource quotas and limits',
            'High availability and scaling',
            'Backup and disaster recovery',
            'Performance optimization',
            'Cost optimization'
        ]
    }
    
    print("Key container concepts for interviews:")
    for concept in summary['key_concepts']:
        print(f"  ✓ {concept}")
    
    print(f"\nDocker vs Kubernetes:")
    for platform, description in summary['docker_vs_kubernetes'].items():
        print(f"  {platform}: {description}")
    
    print(f"\nProduction considerations:")
    for consideration in summary['production_considerations']:
        print(f"  ✓ {consideration}")
    
    return summary

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'kubernetes_docker_dag',
    default_args=default_args,
    schedule=None,  # Manual trigger
    catchup=False,
    tags=["interview", "kubernetes", "docker", "containers"]
)

# Start
start = EmptyOperator(task_id='start', dag=dag)

# Container execution demonstrations
docker_task = PythonOperator(
    task_id='simulate_docker_task',
    python_callable=simulate_docker_task,
    dag=dag
)

kubernetes_task = PythonOperator(
    task_id='simulate_kubernetes_pod',
    python_callable=simulate_kubernetes_pod,
    dag=dag
)

# Container concepts
networking_task = PythonOperator(
    task_id='demonstrate_container_networking',
    python_callable=demonstrate_container_networking,
    dag=dag
)

security_task = PythonOperator(
    task_id='demonstrate_container_security',
    python_callable=demonstrate_container_security,
    dag=dag
)

volume_task = PythonOperator(
    task_id='demonstrate_volume_management',
    python_callable=demonstrate_volume_management,
    dag=dag
)

monitoring_task = PythonOperator(
    task_id='demonstrate_container_monitoring',
    python_callable=demonstrate_container_monitoring,
    dag=dag
)

# Summary
summary_task = PythonOperator(
    task_id='create_container_summary',
    python_callable=create_container_summary,
    dag=dag
)

# Dependencies
start >> [docker_task, kubernetes_task]
[docker_task, kubernetes_task] >> [networking_task, security_task, volume_task, monitoring_task]
[networking_task, security_task, volume_task, monitoring_task] >> summary_task