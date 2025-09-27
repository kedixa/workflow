/*
  Copyright (c) 2019 Sogou, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

  Authors: Wu Jiaxu (wujiaxu@sogou-inc.com)
           Xie Han (xiehan@sogou-inc.com)
*/

#include <stdint.h>
#include <chrono>
#include "DnsCache.h"
#include "DnsUtil.h"

#define GET_CURRENT_SECOND	std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now().time_since_epoch()).count()

#define	TTL_INC				5

static bool __is_expired(const DnsCache::DnsHandle *hdl, int64_t cur, int type)
{
	if (type == GET_TYPE_TTL)
		return cur > hdl->value.expire_time;

	return cur > hdl->value.confident_time;
}

const DnsCache::DnsHandle *DnsCache::get_inner(const HostPort& host_port,
											   int type)
{
	int64_t cur = GET_CURRENT_SECOND;
	const DnsHandle *handle;

	pthread_rwlock_rdlock(&rwlock_);

	auto it = cache_.find(host_port);
	if (it == cache_.end())
	{
		pthread_rwlock_unlock(&rwlock_);
		return NULL;
	}
	else if (!__is_expired(it->second, cur, type))
	{
		handle = it->second;
		handle->inc_ref();

		pthread_rwlock_unlock(&rwlock_);
		return handle;
	}

	pthread_rwlock_unlock(&rwlock_);

	pthread_rwlock_wrlock(&rwlock_);

	it = cache_.find(host_port);
	if (it == cache_.end())
	{
		pthread_rwlock_unlock(&rwlock_);
		return NULL;
	}

	handle = it->second;
	if (__is_expired(handle, cur, type))
	{
		if (!handle->value.delayed())
		{
			DnsHandle *h = const_cast<DnsHandle *>(handle);
			if (type == GET_TYPE_TTL)
				h->value.expire_time += TTL_INC;
			else
				h->value.confident_time += TTL_INC;

			h->value.addrinfo->ai_flags |= 2;
		}

		pthread_rwlock_unlock(&rwlock_);
		return NULL;
	}

	handle->inc_ref();
	pthread_rwlock_unlock(&rwlock_);
	return handle;
}

const DnsCache::DnsHandle *DnsCache::put(const HostPort& host_port,
										 struct addrinfo *addrinfo,
										 unsigned int dns_ttl_default,
										 unsigned int dns_ttl_min)
{
	int64_t expire_time;
	int64_t confident_time;
	int64_t cur_time = GET_CURRENT_SECOND;

	if (dns_ttl_min > dns_ttl_default)
		dns_ttl_min = dns_ttl_default;

	if (dns_ttl_min == (unsigned int)-1)
		confident_time = INT64_MAX;
	else
		confident_time = cur_time + dns_ttl_min;

	if (dns_ttl_default == (unsigned int)-1)
		expire_time = INT64_MAX;
	else
		expire_time = cur_time + dns_ttl_default;

	DnsHandle *handle = new DnsHandle();
	handle->value.addrinfo = addrinfo;
	handle->value.confident_time = confident_time;
	handle->value.expire_time = expire_time;

	pthread_rwlock_wrlock(&rwlock_);

	auto it = cache_.find(host_port);
	if (it != cache_.end())
	{
		it->second->dec_ref();
		it->second = handle;
	}
	else
	{
		cache_.insert({host_port, handle});
	}

	handle->inc_ref();
	pthread_rwlock_unlock(&rwlock_);
	return handle;
}

const DnsCache::DnsHandle *DnsCache::get(const DnsCache::HostPort& host_port)
{
	const DnsHandle *handle = NULL;

	pthread_rwlock_rdlock(&rwlock_);

	auto it = cache_.find(host_port);
	if (it != cache_.end())
	{
		handle = it->second;
		handle->inc_ref();
	}

	pthread_rwlock_unlock(&rwlock_);
	return handle;
}

void DnsCache::release(const DnsCache::DnsHandle *handle)
{
	handle->dec_ref();
}

void DnsCache::del(const DnsCache::HostPort& key)
{
	pthread_rwlock_wrlock(&rwlock_);

	auto it = cache_.find(key);
	if (it != cache_.end())
	{
		it->second->dec_ref();
		cache_.erase(it);
	}

	pthread_rwlock_unlock(&rwlock_);
}

DnsCache::DnsCache()
{
	pthread_rwlock_init(&rwlock_, NULL);
}

DnsCache::~DnsCache()
{
	for (const auto &pair : cache_)
		pair.second->dec_ref();

	pthread_rwlock_destroy(&rwlock_);
}

DnsCache::DnsHandle::~DnsHandle()
{
	struct addrinfo *ai = value.addrinfo;

	if (ai)
	{
		if (ai->ai_flags & 1)
			freeaddrinfo(ai);
		else
			protocol::DnsUtil::freeaddrinfo(ai);
	}
}

