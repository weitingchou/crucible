{{/*
Expand the name of the chart.
*/}}
{{- define "crucible.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "crucible.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Chart label.
*/}}
{{- define "crucible.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "crucible.labels" -}}
helm.sh/chart: {{ include "crucible.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels for a given component.
Usage: include "crucible.selectorLabels" (dict "root" . "component" "control-plane")
*/}}
{{- define "crucible.selectorLabels" -}}
app.kubernetes.io/name: {{ include "crucible.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Effective ECR / image registry URL.
Uses imageRegistry if explicitly set; otherwise auto-builds from awsAccountId + s3.region.
*/}}
{{- define "crucible.imageRegistry" -}}
{{- if .Values.imageRegistry -}}
{{- .Values.imageRegistry -}}
{{- else if .Values.awsAccountId -}}
{{- printf "%s.dkr.ecr.%s.amazonaws.com" .Values.awsAccountId .Values.s3.region -}}
{{- end -}}
{{- end }}

{{/*
IRSA role ARN built from awsAccountId + serviceAccount.irsaRoleName.
*/}}
{{- define "crucible.irsaRoleArn" -}}
{{- if and .Values.awsAccountId .Values.serviceAccount.irsaRoleName -}}
{{- printf "arn:aws:iam::%s:role/%s" .Values.awsAccountId .Values.serviceAccount.irsaRoleName -}}
{{- end -}}
{{- end }}

{{/*
Build a full image reference respecting the global registry override.
Usage: include "crucible.image" (dict "registry" .Values.imageRegistry "image" .Values.controlPlane.image)
*/}}
{{- define "crucible.image" -}}
{{- $registry := .registry -}}
{{- $repo := .image.repository -}}
{{- $tag := .image.tag | default "latest" -}}
{{- if $registry -}}
{{- printf "%s/%s:%s" $registry $repo $tag -}}
{{- else -}}
{{- printf "%s:%s" $repo $tag -}}
{{- end -}}
{{- end }}

{{/*
Name of the main credentials Secret.
*/}}
{{- define "crucible.secretName" -}}
{{- printf "%s-credentials" (include "crucible.fullname" .) }}
{{- end }}

{{/*
RabbitMQ service hostname.
*/}}
{{- define "crucible.rabbitmqHost" -}}
{{- printf "%s-rabbitmq" (include "crucible.fullname" .) }}
{{- end }}

{{/*
PostgreSQL service hostname.
*/}}
{{- define "crucible.postgresHost" -}}
{{- printf "%s-postgresql" (include "crucible.fullname" .) }}
{{- end }}

{{/*
MinIO / S3 endpoint URL (internal cluster URL when MinIO is enabled).
*/}}
{{- define "crucible.s3EndpointUrl" -}}
{{- if .Values.minio.enabled -}}
{{- if .Values.minio.endpointUrl -}}
{{- .Values.minio.endpointUrl }}
{{- else -}}
{{- printf "http://%s-minio:%d" (include "crucible.fullname" .) (.Values.minio.service.s3Port | int) }}
{{- end -}}
{{- end -}}
{{- end }}

{{/*
ServiceAccount name used by control-plane and worker pods.
*/}}
{{- define "crucible.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "crucible.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Prometheus remote-write URL.
*/}}
{{- define "crucible.prometheusRwUrl" -}}
{{- if .Values.prometheus.enabled -}}
{{- printf "http://%s-prometheus:%d/api/v1/write" (include "crucible.fullname" .) (.Values.prometheus.service.port | int) }}
{{- end -}}
{{- end }}
