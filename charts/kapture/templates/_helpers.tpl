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
Local Postgres resource name
*/}}
{{- define "kapture.history.localPostgres.name" -}}
{{- printf "%s-postgres" (include "kapture.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Local Postgres labels
*/}}
{{- define "kapture.history.localPostgres.labels" -}}
{{ include "kapture.labels" . }}
app.kubernetes.io/name: {{ include "kapture.name" . }}-postgres
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: postgres
{{- end }}

{{/*
Local Postgres selector labels
*/}}
{{- define "kapture.history.localPostgres.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kapture.name" . }}-postgres
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Local Postgres connection URL for in-cluster development installs.
*/}}
{{- define "kapture.history.localPostgres.databaseURL" -}}
{{- $username := .Values.history.localPostgres.auth.username | urlquery -}}
{{- $password := .Values.history.localPostgres.auth.password | urlquery -}}
{{- $database := .Values.history.localPostgres.auth.database | urlquery -}}
{{- printf "postgres://%s:%s@%s.%s.svc.cluster.local:%v/%s?sslmode=disable" $username $password (include "kapture.history.localPostgres.name" .) (include "kapture.namespace" .) .Values.history.localPostgres.service.port $database -}}
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

