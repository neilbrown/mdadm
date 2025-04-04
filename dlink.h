
/* doubley linked lists */
/* This is free software. No strings attached. No copyright claimed */

struct __dl_head
{
    void * dh_prev;
    void * dh_next;
};

#define	dl_alloc(size)	((void*)(((char*)xcalloc(1,(size)+sizeof(struct __dl_head)))+sizeof(struct __dl_head)))
#define	dl_new(t)	((t*)dl_alloc(sizeof(t)))
#define	dl_newv(t,n)	((t*)dl_alloc(sizeof(t)*n))

#define dl_next(p) *(&(((struct __dl_head*)(p))[-1].dh_next))
#define dl_prev(p) *(&(((struct __dl_head*)(p))[-1].dh_prev))

void *dl_head(void);
char *dl_strdup(char *s);
char *dl_strndup(char *s, int l);
void dl_insert(void *head, void *val);
void dl_add(void *head, void *val);
void dl_del(void *val);
void dl_free(void *v);
void dl_init(void *v);
void dl_free_all(void *head);
