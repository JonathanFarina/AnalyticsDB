{{/*
Expand the name of the chart.
*/}}
{{- define "analyticsdb.fullname" -}}
{{- default .Chart.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "analyticsdb.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "analyticsdb.labels" -}}
helm.sh/chart: {{ include "analyticsdb.chart" . }}
{{ include "analyticsdb.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "analyticsdb.selectorLabels" -}}
app.kubernetes.io/name: {{ include "analyticsdb.fullname" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
