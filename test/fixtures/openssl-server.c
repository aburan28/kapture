// SPDX-License-Identifier: Apache-2.0
// Native probe fixture: every connection performs public _ex I/O on a worker
// thread. The other native case uses s_server's private SSL BIO helpers.
#include <arpa/inet.h>
#include <openssl/ssl.h>
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

static SSL_CTX *context;

static void *serve(void *argument) {
    int fd = *(int *)argument;
    free(argument);
    SSL *ssl = SSL_new(context);
    if (ssl != NULL && SSL_set_fd(ssl, fd) == 1 && SSL_accept(ssl) == 1) {
        char buffer[4096];
        size_t received = 0, sent = 0;
        const char response[] = "HTTP/1.1 200 OK\r\nContent-Length: 6\r\nSet-Cookie: native-canary\r\nConnection: close\r\n\r\nnative";
        if (SSL_read_ex(ssl, buffer, sizeof(buffer), &received) == 1 && received > 0)
            (void)SSL_write_ex(ssl, response, sizeof(response) - 1, &sent);
        (void)SSL_shutdown(ssl);
    }
    SSL_free(ssl);
    close(fd);
    return NULL;
}

int main(int argc, char **argv) {
    const char *address = NULL, *certificate = NULL, *key = NULL;
    for (int index = 1; index + 1 < argc; index++) {
        if (strcmp(argv[index], "-accept") == 0) address = argv[++index];
        else if (strcmp(argv[index], "-cert") == 0) certificate = argv[++index];
        else if (strcmp(argv[index], "-key") == 0) key = argv[++index];
    }
    if (address == NULL || certificate == NULL || key == NULL) return 2;
    const char *port = strrchr(address, ':');
    if (port == NULL) return 2;
    unsigned value = 0;
    for (port++; *port != '\0'; port++) {
        if (*port < '0' || *port > '9') return 2;
        value = value * 10 + (unsigned)(*port - '0');
        if (value > 65535) return 2;
    }
    signal(SIGPIPE, SIG_IGN);
    context = SSL_CTX_new(TLS_server_method());
    if (context == NULL || SSL_CTX_use_certificate_file(context, certificate, SSL_FILETYPE_PEM) != 1 ||
        SSL_CTX_use_PrivateKey_file(context, key, SSL_FILETYPE_PEM) != 1) return 3;
    int listener = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in endpoint = {.sin_family = AF_INET, .sin_port = htons(value)};
    endpoint.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (listener < 0 || bind(listener, (struct sockaddr *)&endpoint, sizeof(endpoint)) != 0 || listen(listener, 16) != 0) return 4;
    for (;;) {
        int *fd = malloc(sizeof(*fd));
        if (fd == NULL) return 5;
        *fd = accept(listener, NULL, NULL);
        if (*fd < 0) { free(fd); continue; }
        pthread_t thread;
        if (pthread_create(&thread, NULL, serve, fd) != 0) { close(*fd); free(fd); continue; }
        pthread_detach(thread);
    }
}
