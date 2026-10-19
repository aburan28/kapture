// SPDX-License-Identifier: Apache-2.0
// Isolated HTTP/1.1 benchmark fixture, not a production HTTP server.
// One TLS 1.3 connection per worker; exact framing, body and ID oracles.
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <openssl/sha.h>
#include <openssl/ssl.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static SSL_CTX *context;
static int work_rounds;

static int read_all(SSL *ssl, char *buffer, size_t length) {
    while (length) {
        size_t n = 0;
        if (SSL_read_ex(ssl, buffer, length, &n) != 1 || !n) return 0;
        buffer += n;
        length -= n;
    }
    return 1;
}

static void *serve(void *argument) {
    int fd = *(int *)argument;
    free(argument);
    SSL *ssl = SSL_new(context);
    if (!ssl || SSL_set_fd(ssl, fd) != 1 || SSL_accept(ssl) != 1) goto done;
    char buffer[32768];
    for (;;) {
        size_t used = 0, length = 0;
        char id[64] = "", phase[32] = "";
        char *header_end = NULL;
        while (!header_end && used < 8192) {
            size_t n = 0;
            if (SSL_read_ex(ssl, buffer + used, sizeof(buffer) - used - 1, &n) != 1 || !n) goto done;
            used += n;
            buffer[used] = '\0';
            header_end = strstr(buffer, "\r\n\r\n");
        }
        if (!header_end || strncmp(buffer, "POST /bench HTTP/1.1\r\n", 22) != 0) goto done;
        size_t header_bytes = (size_t)(header_end - buffer) + 4;
        char header[8192];
        if (header_bytes >= sizeof(header)) goto done;
        memcpy(header, buffer, header_bytes);
        header[header_bytes] = '\0';
        char *save = NULL;
        for (char *line = strtok_r(header, "\r\n", &save); line; line = strtok_r(NULL, "\r\n", &save)) {
            if (!strncasecmp(line, "Content-Length: ", 16)) length = strtoul(line + 16, NULL, 10);
            else if (!strncasecmp(line, "X-Bench-Id: ", 12)) snprintf(id, sizeof(id), "%s", line + 12);
            else if (!strncasecmp(line, "X-Bench-Phase: ", 15)) snprintf(phase, sizeof(phase), "%s", line + 15);
        }
        if (!length || length > 16300 || !id[0] || !phase[0] || used > header_bytes + length) goto done;
        if (used < header_bytes + length && !read_all(ssl, buffer + used, header_bytes + length - used)) goto done;
        memmove(buffer, buffer + header_bytes, length);
        for (size_t index = 0; index < length; index++) if (buffer[index] != 'x') goto done;
        unsigned char digest[SHA256_DIGEST_LENGTH];
        // Always compute one body digest. CPU-work profile adds repeated body hashes.
        for (int round = 0; round <= work_rounds; round++) SHA256((unsigned char *)buffer, length, digest);
        char hex[SHA256_DIGEST_LENGTH * 2 + 1];
        for (size_t index = 0; index < sizeof(digest); index++) snprintf(hex + index * 2, 3, "%02x", digest[index]);
        int bytes = snprintf(buffer, sizeof(buffer), "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nX-Bench-Id: %s\r\nX-Bench-SHA256: %s\r\nConnection: keep-alive\r\n\r\n", id, hex);
        size_t sent = 0;
        if (bytes <= 0 || SSL_write_ex(ssl, buffer, (size_t)bytes, &sent) != 1 || sent != (size_t)bytes) goto done;
    }
done:
    SSL_free(ssl);
    close(fd);
    return NULL;
}

int main(int argc, char **argv) {
    if (argc != 5) { fprintf(stderr, "usage: openssl-app port cert key hash-rounds\n"); return 2; }
    int port = atoi(argv[1]);
    work_rounds = atoi(argv[4]);
    if (port < 1024 || port > 65535 || work_rounds < 0 || work_rounds > 10000) return 2;
    signal(SIGPIPE, SIG_IGN);
    context = SSL_CTX_new(TLS_server_method());
    if (!context || SSL_CTX_set_min_proto_version(context, TLS1_3_VERSION) != 1 ||
        SSL_CTX_set_max_proto_version(context, TLS1_3_VERSION) != 1 ||
        SSL_CTX_use_certificate_file(context, argv[2], SSL_FILETYPE_PEM) != 1 ||
        SSL_CTX_use_PrivateKey_file(context, argv[3], SSL_FILETYPE_PEM) != 1) return 3;
    int listener = socket(AF_INET, SOCK_STREAM, 0), enabled = 1;
    struct sockaddr_in address = {.sin_family = AF_INET, .sin_port = htons((unsigned short)port)};
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &enabled, sizeof(enabled));
    if (listener < 0 || bind(listener, (struct sockaddr *)&address, sizeof(address)) || listen(listener, 256)) return 4;
    for (;;) {
        int *fd = malloc(sizeof(*fd));
        if (!fd) return 5;
        *fd = accept(listener, NULL, NULL);
        if (*fd < 0) { free(fd); continue; }
        setsockopt(*fd, IPPROTO_TCP, TCP_NODELAY, &enabled, sizeof(enabled));
        pthread_t thread;
        if (pthread_create(&thread, NULL, serve, fd)) { close(*fd); free(fd); continue; }
        pthread_detach(thread);
    }
}
