/*
  Copyright (c) 2025 Sogou, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

  Author: Xie Han (xiehan@sogou-inc.com)
*/

#include <atomic>
#include <mutex>

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>

#include "msgqueue.h"

struct __simple_queue
{
	size_t msg_max;
	size_t msg_cnt;
	int linkoff;
	int nonblock;
	void *head1;
	void *head2;
	void **get_head;
	void **put_head;
	void **put_tail;
	pthread_mutex_t get_mutex;
	pthread_mutex_t put_mutex;
	pthread_cond_t get_cond;
	pthread_cond_t put_cond;
};

void __simple_queue_set_nonblock(__simple_queue *queue)
{
	queue->nonblock = 1;
	pthread_mutex_lock(&queue->put_mutex);
	pthread_cond_signal(&queue->get_cond);
	pthread_cond_broadcast(&queue->put_cond);
	pthread_mutex_unlock(&queue->put_mutex);
}

void __simple_queue_set_block(__simple_queue *queue)
{
	queue->nonblock = 0;
}

void __simple_queue_put(void *msg, __simple_queue *queue)
{
	void **link = (void **)((char *)msg + queue->linkoff);

	*link = NULL;
	pthread_mutex_lock(&queue->put_mutex);
	while (queue->msg_cnt > queue->msg_max - 1 && !queue->nonblock)
		pthread_cond_wait(&queue->put_cond, &queue->put_mutex);

	*queue->put_tail = link;
	queue->put_tail = link;
	queue->msg_cnt++;
	pthread_mutex_unlock(&queue->put_mutex);
	pthread_cond_signal(&queue->get_cond);
}

void __simple_queue_put_head(void *msg, __simple_queue *queue)
{
	void **link = (void **)((char *)msg + queue->linkoff);

	pthread_mutex_lock(&queue->put_mutex);
	while (*queue->get_head)
	{
		if (pthread_mutex_trylock(&queue->get_mutex) == 0)
		{
			pthread_mutex_unlock(&queue->put_mutex);
			*link = *queue->get_head;
			*queue->get_head = link;
			pthread_mutex_unlock(&queue->get_mutex);
			return;
		}
	}

	while (queue->msg_cnt > queue->msg_max - 1 && !queue->nonblock)
		pthread_cond_wait(&queue->put_cond, &queue->put_mutex);

	*link = *queue->put_head;
	if (*link == NULL)
		queue->put_tail = link;

	*queue->put_head = link;
	queue->msg_cnt++;
	pthread_mutex_unlock(&queue->put_mutex);
	pthread_cond_signal(&queue->get_cond);
}

static size_t __simple_queue_swap(__simple_queue *queue)
{
	void **get_head = queue->get_head;
	size_t cnt;

	pthread_mutex_lock(&queue->put_mutex);
	while (queue->msg_cnt == 0 && !queue->nonblock)
		pthread_cond_wait(&queue->get_cond, &queue->put_mutex);

	cnt = queue->msg_cnt;
	if (cnt > queue->msg_max - 1)
		pthread_cond_broadcast(&queue->put_cond);

	queue->get_head = queue->put_head;
	queue->put_head = get_head;
	queue->put_tail = get_head;
	queue->msg_cnt = 0;
	pthread_mutex_unlock(&queue->put_mutex);
	return cnt;
}

void *__simple_queue_get(__simple_queue *queue)
{
	void *msg;

	pthread_mutex_lock(&queue->get_mutex);
	if (*queue->get_head || __simple_queue_swap(queue) > 0)
	{
		msg = (char *)*queue->get_head - queue->linkoff;
		*queue->get_head = *(void **)*queue->get_head;
	}
	else
		msg = NULL;

	pthread_mutex_unlock(&queue->get_mutex);
	return msg;
}

__simple_queue *__simple_queue_create(size_t maxlen, int linkoff)
{
	__simple_queue *queue = (__simple_queue *)malloc(sizeof(__simple_queue));
	int ret;

	if (!queue)
		return NULL;

	ret = pthread_mutex_init(&queue->get_mutex, NULL);
	if (ret == 0)
	{
		ret = pthread_mutex_init(&queue->put_mutex, NULL);
		if (ret == 0)
		{
			ret = pthread_cond_init(&queue->get_cond, NULL);
			if (ret == 0)
			{
				ret = pthread_cond_init(&queue->put_cond, NULL);
				if (ret == 0)
				{
					queue->msg_max = maxlen;
					queue->linkoff = linkoff;
					queue->head1 = NULL;
					queue->head2 = NULL;
					queue->get_head = &queue->head1;
					queue->put_head = &queue->head2;
					queue->put_tail = &queue->head2;
					queue->msg_cnt = 0;
					queue->nonblock = 0;
					return queue;
				}

				pthread_cond_destroy(&queue->get_cond);
			}

			pthread_mutex_destroy(&queue->put_mutex);
		}

		pthread_mutex_destroy(&queue->get_mutex);
	}

	errno = ret;
	free(queue);
	return NULL;
}

void __simple_queue_destroy(__simple_queue *queue)
{
	pthread_cond_destroy(&queue->put_cond);
	pthread_cond_destroy(&queue->get_cond);
	pthread_mutex_destroy(&queue->put_mutex);
	pthread_mutex_destroy(&queue->get_mutex);
	free(queue);
}

constexpr static size_t SUBQUEUE_COUNT = 4;

struct __msgqueue
{
	std::atomic<uint32_t> head;
	char padding1[60];
	std::atomic<uint32_t> tail;
	char padding2[60];

	__simple_queue *ques[SUBQUEUE_COUNT];
};

extern "C"
{

msgqueue_t *msgqueue_create(size_t maxlen, int linkoff)
{
	msgqueue_t *queue = (msgqueue_t *)malloc(sizeof (msgqueue_t));

	if (!queue)
		return NULL;

	queue->head.store(0);
	queue->tail.store(0);

	size_t sublen = SIZE_MAX;
	if (maxlen > 0)
	{
		sublen = maxlen / SUBQUEUE_COUNT;
		if (maxlen % SUBQUEUE_COUNT != 0 && sublen != SIZE_MAX)
			++sublen;
	}

	size_t i;
	for (i = 0; i < SUBQUEUE_COUNT; i++)
	{
		queue->ques[i] = __simple_queue_create(sublen, linkoff);
		if (!queue->ques[i])
			break;
	}

	if (i == SUBQUEUE_COUNT)
		return queue;

	while (i > 0)
		__simple_queue_destroy(queue->ques[--i]);

	free(queue);
	return NULL;
}

void *msgqueue_get(msgqueue_t *queue)
{
	uint32_t i = queue->tail.fetch_add(1) % SUBQUEUE_COUNT;
	return __simple_queue_get(queue->ques[i]);
}

void msgqueue_put(void *msg, msgqueue_t *queue)
{
	uint32_t i = queue->head.fetch_add(1) % SUBQUEUE_COUNT;
	__simple_queue_put(msg, queue->ques[i]);
}

void msgqueue_put_head(void *msg, msgqueue_t *queue)
{
	uint32_t i = queue->head.fetch_add(1) % SUBQUEUE_COUNT;
	__simple_queue_put_head(msg, queue->ques[i]);
}

void msgqueue_set_nonblock(msgqueue_t *queue)
{
	for (size_t i = 0; i < SUBQUEUE_COUNT; i++)
		__simple_queue_set_nonblock(queue->ques[i]);
}

void msgqueue_set_block(msgqueue_t *queue)
{
	for (size_t i = 0; i < SUBQUEUE_COUNT; i++)
		__simple_queue_set_block(queue->ques[i]);
}

void msgqueue_destroy(msgqueue_t *queue)
{
	for (size_t i = 0; i < SUBQUEUE_COUNT; i++)
		__simple_queue_destroy(queue->ques[i]);

	free(queue);
}

}
