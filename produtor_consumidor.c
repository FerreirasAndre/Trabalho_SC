/* pipeline.c
   Implementa P -> CP1 -> CP2 -> CP3 -> C
   Compilar: gcc -o pipeline pipeline.c -pthread
*/

#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define BUFF_SIZE 5
#define NP 1        /* produtores (P) */
#define NCP1 5      /* CP1 threads */
#define NCP2 4      /* CP2 threads */
#define NCP3 3      /* CP3 threads */
#define NC 1        /* consumidores finais (C) */

#define MATRIX_ORDER 10

typedef struct {
    char nome[256];
    double A[MATRIX_ORDER][MATRIX_ORDER];
    double B[MATRIX_ORDER][MATRIX_ORDER];
    double C[MATRIX_ORDER][MATRIX_ORDER];
    double V[MATRIX_ORDER];
    double E;
} S;

/* buffer circular de ponteiros para S */
typedef struct {
    S *buf[BUFF_SIZE];
    int in, out;
    sem_t full;
    sem_t empty;
    sem_t mutex;
} sbuf_t;

/* quatro buffers entre estágios */
sbuf_t shared[4];

int total_files = 0;      /* total de arquivos lidos a partir de entrada.in */
volatile int c_count = 0; /* contador local da thread C (quantos processados) */

pthread_t tidP[NP];
pthread_t tidCP1[NCP1];
pthread_t tidCP2[NCP2];
pthread_t tidCP3[NCP3];
pthread_t tidC[NC];

int argP[NP], argCP1[NCP1], argCP2[NCP2], argCP3[NCP3], argC[NC];

/* utilitários */
void sbuf_init(sbuf_t *sp) {
    sp->in = sp->out = 0;
    sem_init(&sp->full, 0, 0);
    sem_init(&sp->empty, 0, BUFF_SIZE);
    sem_init(&sp->mutex, 0, 1);
}

void sbuf_put(sbuf_t *sp, S *item) {
    sem_wait(&sp->empty);
    sem_wait(&sp->mutex);
    sp->buf[sp->in] = item;
    sp->in = (sp->in + 1) % BUFF_SIZE;
    sem_post(&sp->mutex);
    sem_post(&sp->full);
}

S *sbuf_get(sbuf_t *sp) {
    S *item;
    sem_wait(&sp->full);
    sem_wait(&sp->mutex);
    item = sp->buf[sp->out];
    sp->out = (sp->out + 1) % BUFF_SIZE;
    sem_post(&sp->mutex);
    sem_post(&sp->empty);
    return item;
}

/* leitura de matriz do arquivo: assume MATRIX_ORDER linhas, cada linha com valores
   separados por vírgula (ex: 1.0,2.0,3.0,... ) */
int read_matrix_from_file(FILE *f, double M[MATRIX_ORDER][MATRIX_ORDER]) {
    char line[4096];
    for (int i = 0; i < MATRIX_ORDER; i++) {
        if (!fgets(line, sizeof(line), f)) return -1;
        char *p = line;
        for (int j = 0; j < MATRIX_ORDER; j++) {
            char *end;
            double val = strtod(p, &end);
            M[i][j] = val;
            if (j < MATRIX_ORDER-1) {
                /* p must skip comma or whitespace */
                p = strchr(p, ',');
                if (!p) return -1;
                p++; /* após vírgula */
            }
        }
    }
    return 0;
}

/* escreve matriz no arquivo de saída com espaços entre elementos (formato pedido) */
void write_matrix(FILE *f, double M[MATRIX_ORDER][MATRIX_ORDER]) {
    for (int i = 0; i < MATRIX_ORDER; i++) {
        for (int j = 0; j < MATRIX_ORDER; j++) {
            if (j) fprintf(f, " ");
            fprintf(f, "%.6f", M[i][j]);
        }
        fprintf(f, "\n");
    }
}

/* multiplicação C = A * B */
void multiply_matrix(double A[MATRIX_ORDER][MATRIX_ORDER],
                     double B[MATRIX_ORDER][MATRIX_ORDER],
                     double C[MATRIX_ORDER][MATRIX_ORDER]) {
    for (int i = 0; i < MATRIX_ORDER; i++) {
        for (int j = 0; j < MATRIX_ORDER; j++) {
            C[i][j] = 0.0;
            for (int k = 0; k < MATRIX_ORDER; k++) {
                C[i][j] += A[i][k] * B[k][j];
            }
        }
    }
}

/* soma das colunas de C -> V (V[j] = sum_{i} C[i][j]) */
void sum_columns(double C[MATRIX_ORDER][MATRIX_ORDER], double V[MATRIX_ORDER]) {
    for (int j = 0; j < MATRIX_ORDER; j++) {
        V[j] = 0.0;
        for (int i = 0; i < MATRIX_ORDER; i++) {
            V[j] += C[i][j];
        }
    }
}

/* soma dos elementos de V -> E */
double sum_vector(double V[MATRIX_ORDER]) {
    double s = 0.0;
    for (int i = 0; i < MATRIX_ORDER; i++) s += V[i];
    return s;
}

/* PRODUCER: P
   Lê 'entrada.in' (lista de nomes). Para cada nome, abre o arquivo, cria S, preenche A e B e
   coloca ponteiro em shared[0].
*/
void *Producer(void *arg) {
    (void)arg;
    FILE *list = fopen("entrada.in", "r");
    if (!list) {
        perror("entrada.in");
        return NULL;
    }
    char filename[256];
    int count = 0;
    while (fgets(filename, sizeof(filename), list)) {
        /* retira newline */
        filename[strcspn(filename, "\r\n")] = 0;
        if (strlen(filename) == 0) continue;
        FILE *f = fopen(filename, "r");
        if (!f) {
            fprintf(stderr, "Não foi possível abrir %s\n", filename);
            continue;
        }
        S *s = malloc(sizeof(S));
        strncpy(s->nome, filename, sizeof(s->nome)-1);
        s->nome[sizeof(s->nome)-1] = '\0';
        /* le A e B, cada uma com MATRIX_ORDER linhas de valores separados por vírgula */
        if (read_matrix_from_file(f, s->A) != 0) {
            fprintf(stderr, "Formato inválido em %s (A)\n", filename);
            free(s);
            fclose(f);
            continue;
        }
        /* pula linha em branco possivel */
        /* le B */
        if (read_matrix_from_file(f, s->B) != 0) {
            fprintf(stderr, "Formato inválido em %s (B)\n", filename);
            free(s);
            fclose(f);
            continue;
        }
        fclose(f);

        sbuf_put(&shared[0], s);
        count++;
    }
    fclose(list);
    total_files = count; /* total de arquivos lidos */
    printf("[P] finalizou. Total lidos = %d\n", total_files);
    return NULL;
}

/* CP1: consome shared[0], calcula C = A*B e produz em shared[1] */
void *CP1(void *arg) {
    int id = *((int *)arg);
    while (1) {
        /* tenta obter item; se nenhum mais existir a thread ficará bloqueada.
           Ela será "despertada" quando o pai liberar os semáforos após C terminar. */
        S *s = sbuf_get(&shared[0]);
        if (!s) continue;
        /* calcula C */
        multiply_matrix(s->A, s->B, s->C);
        printf("[CP1_%d] Processou %s -> calculou C\n", id, s->nome);
        sbuf_put(&shared[1], s);
    }
    return NULL;
}

/* CP2: consome shared[1], calcula V (soma das colunas de C) e produz em shared[2] */
void *CP2(void *arg) {
    int id = *((int *)arg);
    while (1) {
        S *s = sbuf_get(&shared[1]);
        if (!s) continue;
        sum_columns(s->C, s->V);
        printf("[CP2_%d] Processou %s -> calculou V\n", id, s->nome);
        sbuf_put(&shared[2], s);
    }
    return NULL;
}

/* CP3: consome shared[2], calcula E (soma de V) e produz em shared[3] */
void *CP3(void *arg) {
    int id = *((int *)arg);
    while (1) {
        S *s = sbuf_get(&shared[2]);
        if (!s) continue;
        s->E = sum_vector(s->V);
        printf("[CP3_%d] Processou %s -> calculou E=%.6f\n", id, s->nome, s->E);
        sbuf_put(&shared[3], s);
    }
    return NULL;
}

/* Consumer final: consome shared[3] e escreve em saida.out.
   Termina quando processa 'total_files' elementos.
*/
void *Consumer(void *arg) {
    (void)arg;
    FILE *out = fopen("saida.out", "w");
    if (!out) {
        perror("saida.out");
        return NULL;
    }

    int local_count = 0;
    while (1) {
        S *s = sbuf_get(&shared[3]);
        if (!s) continue;

        /* escreve no arquivo conforme formato pedido */
        fprintf(out, "================================\n");
        fprintf(out, "Entrada: %s;\n", s->nome);
        fprintf(out, "--------------------------\n");
        fprintf(out, "A\n");
        write_matrix(out, s->A);
        fprintf(out, "--------------------------\n");
        fprintf(out, "B\n");
        write_matrix(out, s->B);
        fprintf(out, "--------------------------\n");
        fprintf(out, "C\n");
        write_matrix(out, s->C);
        fprintf(out, "--------------------------\n");
        fprintf(out, "V\n");
        for (int i = 0; i < MATRIX_ORDER; i++) {
            fprintf(out, "%.6f\n", s->V[i]);
        }
        fprintf(out, "--------------------------\n");
        fprintf(out, "E\n");
        fprintf(out, "%.6f\n", s->E);
        fprintf(out, "================================\n");

        fflush(out);

        local_count++;
        c_count++;

        printf("[C] Escreveu %s (contador C = %d)\n", s->nome, local_count);

        free(s); /* libera memória da estrutura */

        if (total_files > 0 && local_count >= total_files) {
            printf("[C] Processou todos os arquivos (%d). Finalizando C.\n", total_files);
            break;
        }
    }

    fclose(out);
    return NULL;
}

int main() {
    /* inicializa buffers */
    for (int i = 0; i < 4; i++) sbuf_init(&shared[i]);

    /* cria threads */
    for (int i = 0; i < NP; i++) {
        argP[i] = i;
        pthread_create(&tidP[i], NULL, Producer, &argP[i]);
    }
    for (int i = 0; i < NCP1; i++) {
        argCP1[i] = i;
        pthread_create(&tidCP1[i], NULL, CP1, &argCP1[i]);
    }
    for (int i = 0; i < NCP2; i++) {
        argCP2[i] = i;
        pthread_create(&tidCP2[i], NULL, CP2, &argCP2[i]);
    }
    for (int i = 0; i < NCP3; i++) {
        argCP3[i] = i;
        pthread_create(&tidCP3[i], NULL, CP3, &argCP3[i]);
    }
    for (int i = 0; i < NC; i++) {
        argC[i] = i;
        pthread_create(&tidC[i], NULL, Consumer, &argC[i]);
    }

    /* esperar produtor finalizar (opcional) */
    for (int i = 0; i < NP; i++) pthread_join(tidP[i], NULL);
    printf("[main] Producer já terminou. Aguardando C completar os %d arquivos.\n", total_files);

    /* esperar a thread C terminar (somente 1 C neste trabalho) */
    for (int i = 0; i < NC; i++) pthread_join(tidC[i], NULL);

    /* a partir daqui, C já processou tudo; vamos encerrar as outras threads.
       Estratégia simples: descongelar threads bloqueadas postando nos semáforos 'full' */
    printf("[main] C terminou. Liberando CP threads e encerrando.\n");

    /* postar em todos os buffers para desbloquear CPs; repetir BUFF_SIZE vezes para segurança */
    for (int k = 0; k < BUFF_SIZE; k++) {
        for (int b = 0; b < 4; b++) {
            sem_post(&shared[b].full);
        }
    }

    /* cancelar e juntar as threads CP (elas saem pelo cancelamento) */
    for (int i = 0; i < NCP1; i++) pthread_cancel(tidCP1[i]);
    for (int i = 0; i < NCP2; i++) pthread_cancel(tidCP2[i]);
    for (int i = 0; i < NCP3; i++) pthread_cancel(tidCP3[i]);

    for (int i = 0; i < NCP1; i++) pthread_join(tidCP1[i], NULL);
    for (int i = 0; i < NCP2; i++) pthread_join(tidCP2[i], NULL);
    for (int i = 0; i < NCP3; i++) pthread_join(tidCP3[i], NULL);

    /* destruir semáforos */
    for (int i = 0; i < 4; i++) {
        sem_destroy(&shared[i].full);
        sem_destroy(&shared[i].empty);
        sem_destroy(&shared[i].mutex);
    }

    printf("[main] Finalizado com sucesso.\n");
    return 0;
}
