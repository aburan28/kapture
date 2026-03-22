{{/*
Expand the name of the chart.
*/}}
{{- define "kapture.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "kapture.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "kapture.labels" -}}
helm.sh/chart: {{ include "kapture.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Chart label
*/}}
{{- define "kapture.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Hub labels
*/}}
{{- define "kapture.hub.labels" -}}
{{ include "kapture.labels" . }}
app.kubernetes.io/name: {{ include "kapture.name" . }}-hub
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: hub
{{- end }}

{{/*
Hub selector labels
*/}}
{{- define "kapture.hub.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kapture.name" . }}-hub
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Spoke labels
*/}}
{{- define "kapture.spoke.labels" -}}
{{ include "kapture.labels" . }}
app.kubernetes.io/name: {{ include "kapture.name" . }}-spoke
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: spoke
{{- end }}

{{/*
Spoke selector labels
*/}}
{{- define "kapture.spoke.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kapture.name" . }}-spoke
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Hub ServiceAccount name
*/}}
{{- define "kapture.hub.serviceAccountName" -}}
{{- if .Values.hub.serviceAccount.create }}
{{- default (printf "%s-hub" (include "kapture.fullname" .)) }}
{{- else }}
{{- default "default" }}
{{- end }}
{{- end }}

{{/*
Spoke ServiceAccount name
*/}}
{{- define "kapture.spoke.serviceAccountName" -}}
{{- if .Values.spoke.serviceAccount.create }}
{{- default (printf "%s-spoke" (include "kapture.fullname" .)) }}
{{- else }}
{{- default "default" }}
{{- end }}
{{- end }}

{{/*
Namespace helper
*/}}
{{- define "kapture.namespace" -}}
{{- default .Release.Namespace .Values.namespace }}
{{- end }}

