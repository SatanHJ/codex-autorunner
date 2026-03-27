import assert from "node:assert/strict";
import { test } from "node:test";
import { JSDOM } from "jsdom";

const dom = new JSDOM(
  `<!doctype html>
  <html lang="en">
    <head>
      <meta name="description" content="" />
      <title></title>
    </head>
    <body>
      <div id="toast"></div>
      <label class="lang-switch-label" for="repo-language-select">Language</label>
      <select id="repo-language-select" data-car-language-select>
        <option value="en">English</option>
        <option value="zh-CN">简体中文</option>
      </select>
      <button id="hub-new-repo"></button>
      <textarea id="messages-reply-body"></textarea>
      <button id="ticket-flow-bootstrap"></button>
    </body>
  </html>`,
  { url: "http://localhost/repos/demo/" }
);

globalThis.window = dom.window;
globalThis.document = dom.window.document;
globalThis.HTMLElement = dom.window.HTMLElement;
globalThis.Node = dom.window.Node;
globalThis.Event = dom.window.Event;
globalThis.CustomEvent = dom.window.CustomEvent;
globalThis.localStorage = dom.window.localStorage;
globalThis.sessionStorage = dom.window.sessionStorage;
try {
  globalThis.navigator = dom.window.navigator;
} catch {
  // Node 24+ exposes a read-only navigator.
}

const { initI18n, setLanguage, getLanguage, __i18nTest } = await import(
  "../../src/codex_autorunner/static/generated/i18n.js"
);

test("i18n switches UI copy to Chinese and persists language selection", async () => {
  globalThis.localStorage.clear();
  initI18n();

  assert.equal(getLanguage(), "en");
  assert.equal(document.documentElement.lang, "en");
  assert.equal(document.querySelector(".lang-switch-label")?.textContent, "Language");
  assert.equal(document.getElementById("hub-new-repo")?.textContent, "+ New repo");
  assert.equal(
    document.getElementById("messages-reply-body")?.getAttribute("placeholder"),
    "Write a reply…"
  );

  await setLanguage("zh-CN");

  assert.equal(getLanguage(), "zh-CN");
  assert.equal(document.documentElement.lang, "zh-CN");
  assert.equal(document.querySelector(".lang-switch-label")?.textContent, "语言");
  assert.equal(document.getElementById("hub-new-repo")?.textContent, "+ 新建仓库");
  assert.equal(
    document.getElementById("messages-reply-body")?.getAttribute("placeholder"),
    "写一条回复…"
  );
  assert.equal(
    globalThis.localStorage.getItem(__i18nTest.STORAGE_KEY),
    "zh-CN"
  );
  assert.equal(
    document.getElementById("repo-language-select")?.value,
    "zh-CN"
  );
});

test("i18n restores persisted language on later init", () => {
  globalThis.localStorage.setItem(__i18nTest.STORAGE_KEY, "zh-CN");

  initI18n();

  assert.equal(getLanguage(), "zh-CN");
  assert.equal(document.title, "Codex Autorunner");
  assert.equal(
    document.querySelector('meta[name="description"]')?.getAttribute("content"),
    "Codex Autorunner - 自主编码代理编排器"
  );
  assert.equal(document.getElementById("ticket-flow-bootstrap")?.textContent, "启动工单流程");
});
