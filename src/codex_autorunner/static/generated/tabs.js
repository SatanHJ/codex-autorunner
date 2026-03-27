// GENERATED FILE - do not edit directly. Source: static_src/
import { publish } from "./bus.js";
import { escapeHtml, getUrlParams, updateUrlParams } from "./utils.js";
import { onLanguageChange, t } from "./i18n.js";
const tabs = [];
const hamburgerActions = [];
let hamburgerMenuOpen = false;
let hamburgerMenuEl = null;
let hamburgerBtnEl = null;
let hamburgerBackdropEl = null;
let languageSyncBound = false;
export function registerTab(id, label, opts = {}) {
    tabs.push({
        id,
        label,
        i18nKey: opts.i18nKey,
        hidden: Boolean(opts.hidden),
        menuTab: Boolean(opts.menuTab),
        icon: opts.icon,
    });
}
export function registerHamburgerAction(id, label, icon, onClick, opts = {}) {
    hamburgerActions.push({ id, label, i18nKey: opts.i18nKey, icon, onClick });
}
let setActivePanelFn = null;
let pendingActivate = null;
export function activateTab(id) {
    if (setActivePanelFn) {
        setActivePanelFn(id);
    }
    else {
        pendingActivate = id;
    }
}
function closeHamburgerMenu() {
    if (!hamburgerMenuOpen)
        return;
    hamburgerMenuOpen = false;
    hamburgerMenuEl?.classList.remove("open");
    hamburgerBtnEl?.classList.remove("active");
    hamburgerBackdropEl?.classList.remove("open");
}
function toggleHamburgerMenu() {
    hamburgerMenuOpen = !hamburgerMenuOpen;
    hamburgerMenuEl?.classList.toggle("open", hamburgerMenuOpen);
    hamburgerBtnEl?.classList.toggle("active", hamburgerMenuOpen);
    hamburgerBackdropEl?.classList.toggle("open", hamburgerMenuOpen);
}
function updateHamburgerActiveState(activeTabId) {
    if (!hamburgerMenuEl)
        return;
    const items = hamburgerMenuEl.querySelectorAll(".hamburger-item[data-target]");
    items.forEach((item) => {
        const target = item.dataset.target;
        item.classList.toggle("active", target === activeTabId);
    });
    // Also update hamburger button active state if a menu tab is active
    const isMenuTabActive = tabs.some((t) => t.menuTab && t.id === activeTabId);
    hamburgerBtnEl?.classList.toggle("has-active", isMenuTabActive);
}
function renderLabel(label, i18nKey) {
    return escapeHtml(i18nKey ? t(i18nKey) : label);
}
function refreshRenderedLabels() {
    const container = document.querySelector(".tabs");
    container?.querySelectorAll(".tab").forEach((button) => {
        const tab = tabs.find((entry) => entry.id === button.dataset.target);
        const label = button.querySelector(".tab-label");
        if (tab && label) {
            label.textContent = tab.i18nKey ? t(tab.i18nKey) : tab.label;
        }
    });
    hamburgerMenuEl?.querySelectorAll(".hamburger-item[data-target]").forEach((item) => {
        const tab = tabs.find((entry) => entry.id === item.dataset.target);
        const label = item.querySelector("span:last-child");
        if (tab && label) {
            label.textContent = tab.i18nKey ? t(tab.i18nKey) : tab.label;
        }
    });
    hamburgerMenuEl?.querySelectorAll(".hamburger-item[data-action]").forEach((item) => {
        const action = hamburgerActions.find((entry) => entry.id === item.dataset.action);
        const label = item.querySelector("span:last-child");
        if (action && label) {
            label.textContent = action.i18nKey ? t(action.i18nKey) : action.label;
        }
    });
    if (hamburgerBtnEl) {
        hamburgerBtnEl.setAttribute("aria-label", t("nav.more"));
    }
}
export function initTabs(defaultTab = "analytics") {
    const container = document.querySelector(".tabs");
    const navBar = document.querySelector(".nav-bar");
    if (!container)
        return;
    container.innerHTML = "";
    const panels = document.querySelectorAll(".panel");
    const setActivePanel = (id) => {
        panels.forEach((p) => p.classList.toggle("active", p.id === id));
        // Update primary tab buttons
        const buttons = container.querySelectorAll(".tab");
        buttons.forEach((btn) => btn.classList.toggle("active", btn.dataset.target === id));
        // Update hamburger menu items
        updateHamburgerActiveState(id);
        updateUrlParams({ tab: id });
        publish("tab:change", id);
    };
    setActivePanelFn = setActivePanel;
    // Separate primary tabs from menu tabs
    const primaryTabs = tabs.filter((t) => !t.hidden && !t.menuTab);
    const menuTabs = tabs.filter((t) => !t.hidden && t.menuTab);
    // Render primary tabs
    primaryTabs.forEach(tab => {
        const btn = document.createElement("button");
        btn.className = "tab";
        btn.dataset.target = tab.id;
        btn.innerHTML = `
      <span class="tab-label">${renderLabel(tab.label, tab.i18nKey)}</span>
      <span class="badge hidden" id="tab-badge-${tab.id}"></span>
    `;
        btn.addEventListener("click", () => setActivePanel(tab.id));
        container.appendChild(btn);
    });
    // Create hamburger menu if there are menu tabs or actions
    if (menuTabs.length > 0 || hamburgerActions.length > 0) {
        const wrapper = document.createElement("div");
        wrapper.className = "hamburger-wrapper";
        // Hamburger button
        const btn = document.createElement("button");
        btn.className = "hamburger-btn";
        btn.setAttribute("aria-label", t("nav.more"));
        btn.setAttribute("aria-expanded", "false");
        btn.innerHTML = `
      <span class="hamburger-icon">
        <span></span>
        <span></span>
        <span></span>
      </span>
    `;
        hamburgerBtnEl = btn;
        // Hamburger menu dropdown
        const menu = document.createElement("div");
        menu.className = "hamburger-menu";
        menu.setAttribute("role", "menu");
        hamburgerMenuEl = menu;
        // Add menu tab items
        menuTabs.forEach((tab) => {
            const item = document.createElement("button");
            item.className = "hamburger-item";
            item.dataset.target = tab.id;
            item.setAttribute("role", "menuitem");
            const iconHtml = tab.icon ? `<span class="hamburger-item-icon">${tab.icon}</span>` : "";
            item.innerHTML = `${iconHtml}<span>${renderLabel(tab.label, tab.i18nKey)}</span>`;
            item.addEventListener("click", () => {
                setActivePanel(tab.id);
                closeHamburgerMenu();
            });
            menu.appendChild(item);
        });
        // Add divider if there are both tabs and actions
        if (menuTabs.length > 0 && hamburgerActions.length > 0) {
            const divider = document.createElement("div");
            divider.className = "hamburger-divider";
            menu.appendChild(divider);
        }
        // Add action items (like Settings)
        hamburgerActions.forEach((action) => {
            const item = document.createElement("button");
            item.className = "hamburger-item";
            item.dataset.action = action.id;
            item.setAttribute("role", "menuitem");
            const iconHtml = action.icon ? `<span class="hamburger-item-icon">${action.icon}</span>` : "";
            item.innerHTML = `${iconHtml}<span>${renderLabel(action.label, action.i18nKey)}</span>`;
            item.addEventListener("click", () => {
                action.onClick();
                closeHamburgerMenu();
            });
            menu.appendChild(item);
        });
        // Mobile backdrop - appended to body for proper z-index stacking
        const backdrop = document.createElement("div");
        backdrop.className = "hamburger-backdrop";
        backdrop.addEventListener("click", closeHamburgerMenu);
        hamburgerBackdropEl = backdrop;
        document.body.appendChild(backdrop);
        // Append menu to body for mobile z-index stacking (above backdrop)
        // On mobile, the nav-bar has z-index:100 which creates a stacking context
        // that would trap the menu below the backdrop (z-index:1999)
        document.body.appendChild(menu);
        // Toggle menu on button click
        const toggleHandler = (e) => {
            e.stopPropagation();
            // Prevent ghost clicks on touch devices
            if (e.type === "touchend") {
                e.preventDefault();
            }
            toggleHamburgerMenu();
            btn.setAttribute("aria-expanded", String(hamburgerMenuOpen));
        };
        btn.addEventListener("click", toggleHandler);
        btn.addEventListener("touchend", toggleHandler);
        // Close menu on outside click (check both wrapper and menu since menu is in body)
        document.addEventListener("click", (e) => {
            if (hamburgerMenuOpen && !wrapper.contains(e.target) && !menu.contains(e.target)) {
                closeHamburgerMenu();
            }
        });
        // Close menu on Escape
        document.addEventListener("keydown", (e) => {
            if (e.key === "Escape" && hamburgerMenuOpen) {
                closeHamburgerMenu();
                hamburgerBtnEl?.focus();
            }
        });
        wrapper.appendChild(btn);
        // Insert hamburger after tabs or at the end of nav bar
        const navActions = navBar?.querySelector(".nav-actions");
        if (navActions) {
            navBar?.insertBefore(wrapper, navActions);
        }
        else {
            navBar?.appendChild(wrapper);
        }
    }
    const params = getUrlParams();
    const requested = params.get("tab");
    const allVisibleTabs = tabs.filter((t) => !t.hidden);
    const initialTab = allVisibleTabs.some((t) => t.id === requested)
        ? requested
        : allVisibleTabs.some((t) => t.id === defaultTab)
            ? defaultTab
            : allVisibleTabs[0]?.id;
    if (initialTab) {
        setActivePanel(initialTab);
    }
    else if (allVisibleTabs.length > 0) {
        setActivePanel(allVisibleTabs[0].id);
    }
    if (pendingActivate && allVisibleTabs.some((t) => t.id === pendingActivate)) {
        const id = pendingActivate;
        pendingActivate = null;
        setActivePanel(id);
    }
    if (!languageSyncBound) {
        onLanguageChange(() => {
            refreshRenderedLabels();
        });
        languageSyncBound = true;
    }
}
