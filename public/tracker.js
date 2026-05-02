(function (window) {
  'use strict';

  if (!window || !window.document) return;

  var document = window.document;
  var script = document.currentScript;
  var previous = window.simpletrack;
  var queuedCalls = previous && previous.q ? previous.q : [];
  var maxPropertyCount = 64;
  var maxPropertyKeyLength = 128;
  var maxPropertyStringLength = 2048;
  var propertyKeyPattern = /^[A-Za-z0-9][A-Za-z0-9_.:-]*$/;
  var attributionParameters = {
    utm_source: 'utm.source',
    utm_medium: 'utm.medium',
    utm_campaign: 'utm.campaign',
    utm_term: 'utm.term',
    utm_content: 'utm.content',
    utm_id: 'utm.id',
  };
  var clickIDParameters = {
    gclid: 'click.gclid',
    dclid: 'click.dclid',
    gbraid: 'click.gbraid',
    wbraid: 'click.wbraid',
    fbclid: 'click.fbclid',
    msclkid: 'click.msclkid',
    ttclid: 'click.ttclid',
    li_fat_id: 'click.li_fat_id',
  };

  function attr(name) {
    if (!script || typeof script.getAttribute !== 'function') return '';
    return script.getAttribute('data-' + name) || '';
  }

  var config = {
    writeKey: attr('write-key'),
    collectURL: attr('collect-url') || defaultCollectURL(script),
    autoTrack: attr('auto-track') !== 'false',
    trackHistory: attr('track-history') !== 'false',
    respectDoNotTrack: attr('do-not-track') === 'true',
    debug: attr('debug') === 'true',
    credentials: attr('fetch-credentials') || 'omit',
  };
  var identityStorageKey = scopedIdentityStorageKey(config);

  var state = {
    distinctID: trackingSuppressed() ? randomToken('dst') : initialDistinctID(identityStorageKey),
    lastPageURL: '',
  };

  function scopedIdentityStorageKey(currentConfig) {
    return [
      'simpletrack.distinct_id',
      currentConfig.writeKey,
    ].join('.');
  }

  function defaultCollectURL(currentScript) {
    if (!currentScript || !currentScript.src) return '/collect';
    try {
      var url = new window.URL(currentScript.src, window.location.href);
      return url.origin + '/collect';
    } catch (_err) {
      return '/collect';
    }
  }

  function initialDistinctID(key) {
    var storage = safeLocalStorage();
    if (storage) {
      var existing = storageGet(storage, key);
      if (existing) return existing;
      var created = randomToken('dst');
      storageSet(storage, key, created);
      return created;
    }
    return randomToken('dst');
  }

  function safeLocalStorage() {
    try {
      return window.localStorage || null;
    } catch (_err) {
      return null;
    }
  }

  function storageGet(storage, key) {
    try {
      return storage.getItem(key);
    } catch (_err) {
      return null;
    }
  }

  function storageSet(storage, key, value) {
    try {
      storage.setItem(key, value);
      return true;
    } catch (_err) {
      return false;
    }
  }

  function randomToken(prefix) {
    var bytes = new Uint8Array(16);
    if (window.crypto && typeof window.crypto.getRandomValues === 'function') {
      window.crypto.getRandomValues(bytes);
    } else {
      for (var index = 0; index < bytes.length; index += 1) {
        bytes[index] = Math.floor(Math.random() * 256);
      }
    }
    var body = Array.prototype.map
      .call(bytes, function (value) {
        return value.toString(16).padStart(2, '0');
      })
      .join('');
    return prefix + '_' + body;
  }

  function currentURL() {
    try {
      return new window.URL(window.location.href).toString();
    } catch (_err) {
      return String(window.location.href || '');
    }
  }

  function cleanPageURL() {
    try {
      var url = new window.URL(window.location.href);
      url.search = '';
      url.hash = '';
      return url.toString();
    } catch (_err) {
      return String(window.location.origin || '') + String(window.location.pathname || '');
    }
  }

  function baseProperties() {
    var screenValue =
      window.screen && window.screen.width && window.screen.height
        ? window.screen.width + 'x' + window.screen.height
        : '';

    var values = {
      'page.title': document.title || '',
      'page.url': cleanPageURL(),
      'page.path': window.location.pathname || '',
      'page.hostname': window.location.hostname || '',
      'page.referrer': document.referrer || '',
      screen: screenValue,
      language: (window.navigator && window.navigator.language) || '',
    };

    addQueryProperties(values, attributionParameters);
    addQueryProperties(values, clickIDParameters);
    return sanitizeProperties(values);
  }

  function addQueryProperties(values, mapping) {
    try {
      var url = new window.URL(window.location.href);
      Object.keys(mapping).forEach(function (queryName) {
        var value = url.searchParams.get(queryName);
        if (value) {
          values[mapping[queryName]] = value;
        }
      });
    } catch (_err) {
      return;
    }
  }

  function sanitizeProperties(values) {
    if (!values || typeof values !== 'object' || Array.isArray(values)) {
      return undefined;
    }

    var output = {};
    var count = 0;
    Object.keys(values).forEach(function (key) {
      if (count >= maxPropertyCount) return;
      if (!isSupportedPropertyKey(key)) {
        debug('skip invalid property key', key);
        return;
      }
      var value = sanitizePropertyValue(values[key]);
      if (typeof value === 'undefined') {
        debug('skip unsupported property value', key);
        return;
      }
      output[key] = value;
      count += 1;
    });

    return count > 0 ? output : undefined;
  }

  function isSupportedPropertyKey(key) {
    return (
      typeof key === 'string' &&
      key.length > 0 &&
      key.length <= maxPropertyKeyLength &&
      propertyKeyPattern.test(key)
    );
  }

  function sanitizePropertyValue(value) {
    if (value === null || typeof value === 'boolean') return value;
    if (typeof value === 'string') {
      return value.slice(0, maxPropertyStringLength);
    }
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    return undefined;
  }

  function mergeProperties(base, extra) {
    var merged = {};
    var count = 0;
    [base, extra].forEach(function (values) {
      if (!values) return;
      Object.keys(values).forEach(function (key) {
        if (count >= maxPropertyCount || Object.prototype.hasOwnProperty.call(merged, key)) {
          return;
        }
        merged[key] = values[key];
        count += 1;
      });
    });
    return count > 0 ? merged : undefined;
  }

  function validConfig() {
    return config.writeKey && config.collectURL;
  }

  function trackingSuppressed() {
    return config.respectDoNotTrack && hasDoNotTrack();
  }

  function hasDoNotTrack() {
    var navigatorValue = window.navigator && window.navigator.doNotTrack;
    var msValue = window.navigator && window.navigator.msDoNotTrack;
    var windowValue = window.doNotTrack;
    var value = navigatorValue || msValue || windowValue;
    return value === '1' || value === 1 || value === 'yes';
  }

  function validEventName(eventName) {
    return (
      typeof eventName === 'string' &&
      eventName.length > 0 &&
      eventName.length <= 128 &&
      /^[A-Za-z0-9][A-Za-z0-9_.:-]*$/.test(eventName)
    );
  }

  function buildRequest(eventName, properties, userProperties) {
    return {
      id: randomToken('evt'),
      write_key: config.writeKey,
      event_name: eventName,
      distinct_id: state.distinctID,
      event_time: new Date().toISOString(),
      source: 'simpletrack-browser',
      properties: mergeProperties(baseProperties(), sanitizeProperties(properties)),
      user_properties: sanitizeProperties(userProperties),
    };
  }

  function send(eventName, properties, userProperties) {
    if (!validConfig()) {
      debug('missing tracker configuration');
      return Promise.resolve(null);
    }
    if (trackingSuppressed()) {
      debug('do not track is enabled');
      return Promise.resolve(null);
    }
    if (!validEventName(eventName)) {
      debug('invalid event name', eventName);
      return Promise.resolve(null);
    }
    if (typeof window.fetch !== 'function') {
      debug('fetch is unavailable');
      return Promise.resolve(null);
    }

    var request = buildRequest(eventName, properties, userProperties);
    debug('send', request);

    return window
      .fetch(config.collectURL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(request),
        credentials: config.credentials,
        keepalive: true,
      })
      .then(function (response) {
        return response;
      })
      .catch(function (err) {
        debug('send failed', err && err.message ? err.message : err);
        return null;
      });
  }

  function pageview(properties) {
    state.lastPageURL = currentURL();
    return send('pageview', properties);
  }

  function track(eventName, properties) {
    if (!validEventName(eventName)) {
      debug('invalid track event name', eventName);
      return Promise.resolve(null);
    }
    return send(eventName, properties);
  }

  function identify(distinctID, userProperties) {
    if (typeof distinctID === 'string' && distinctID.length > 0) {
      state.distinctID = distinctID;
      if (!trackingSuppressed()) {
        var storage = safeLocalStorage();
        if (storage) {
          storageSet(storage, identityStorageKey, distinctID);
        }
      }
    }
    return send('identify', undefined, userProperties);
  }

  function debug() {
    if (!config.debug || !window.console || typeof window.console.debug !== 'function') return;
    var args = Array.prototype.slice.call(arguments);
    args.unshift('[simpletrack]');
    window.console.debug.apply(window.console, args);
  }

  function onReady(callback) {
    if (document.readyState === 'loading' && typeof document.addEventListener === 'function') {
      document.addEventListener('DOMContentLoaded', callback, { once: true });
      return;
    }
    window.setTimeout(callback, 0);
  }

  function installHistoryTracking() {
    if (!config.trackHistory || !window.history) return;
    ['pushState', 'replaceState'].forEach(function (method) {
      var original = window.history[method];
      if (typeof original !== 'function') return;
      window.history[method] = function () {
        var result = original.apply(window.history, arguments);
        scheduleRoutePageview();
        return result;
      };
    });
    if (typeof window.addEventListener === 'function') {
      window.addEventListener('popstate', scheduleRoutePageview);
    }
  }

  function scheduleRoutePageview() {
    window.setTimeout(function () {
      var nextURL = currentURL();
      if (nextURL !== state.lastPageURL) {
        pageview();
      }
    }, 0);
  }

  function replayQueuedCalls(api) {
    Array.prototype.forEach.call(queuedCalls, function (call) {
      var args = Array.prototype.slice.call(call);
      var method = args.shift();
      if (api[method]) {
        api[method].apply(api, args);
      }
    });
  }

  var api = {
    pageview: pageview,
    track: track,
    identify: identify,
    q: [],
  };

  window.simpletrack = api;
  replayQueuedCalls(api);
  installHistoryTracking();

  if (config.autoTrack) {
    onReady(function () {
      pageview();
    });
  }
})(window);
