import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { JSDOM } from "jsdom";

function installDom(markup, url = "http://localhost/repos/demo/") {
  const dom = new JSDOM(markup, { url });
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
  return dom;
}

function installBasicDom() {
  installDom(`<!doctype html>
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
  </html>`);
}

installBasicDom();

const { initI18n, setLanguage, getLanguage, __i18nTest } = await import(
  "../../src/codex_autorunner/static/generated/i18n.js"
);

test("i18n switches UI copy to Chinese and persists language selection", async () => {
  installBasicDom();
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
  assert.equal(globalThis.localStorage.getItem(__i18nTest.STORAGE_KEY), "zh-CN");
  assert.equal(document.getElementById("repo-language-select")?.value, "zh-CN");
});

test("i18n restores persisted language on later init", () => {
  installBasicDom();
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

test("i18n translates core full-page chrome across hub, repo, and settings shells", async () => {
  const html = readFileSync(
    new URL("../../src/codex_autorunner/static/index.html", import.meta.url),
    "utf8"
  );
  installDom(html);
  globalThis.localStorage.clear();

  initI18n();
  await setLanguage("zh-CN");

  assert.equal(
    document.querySelector('#hub-flow-filter option[value="running"]')?.textContent,
    "运行中"
  );
  assert.equal(document.querySelector("#run-history summary .label")?.textContent, "运行历史");
  assert.equal(
    document.getElementById("messages-filebox-upload-btn")?.getAttribute("title"),
    "上传到收件箱"
  );
  assert.equal(document.getElementById("contextspace-save")?.textContent, "保存");
  assert.equal(document.getElementById("ticket-flow-reconnect")?.textContent, "重新连接");
  assert.equal(document.querySelector(".ticket-template-preview-label")?.textContent, "预览");
  assert.equal(document.querySelector('label[for="terminal-textarea"]')?.textContent, "文本输入");
  assert.equal(
    document.getElementById("terminal-textarea")?.getAttribute("placeholder"),
    "输入或粘贴要发送到终端的文本/图片…"
  );
  assert.equal(document.getElementById("repo-update-btn")?.textContent, "更新 CAR");
  assert.equal(document.getElementById("repo-language-select")?.value, "zh-CN");
});
