(function () {
  const STORAGE_KEY = 'codexmbs-theme';
  const root = document.documentElement;
  const toggle = document.getElementById('themeToggle');
  const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');

  function setTheme(mode) {
    root.setAttribute('data-theme', mode);
    localStorage.setItem(STORAGE_KEY, mode);
    if (toggle) {
      toggle.textContent = mode === 'dark' ? 'Light Mode' : 'Dark Mode';
      toggle.setAttribute('aria-pressed', mode === 'dark' ? 'true' : 'false');
      toggle.setAttribute('title', mode === 'dark' ? 'Switch to light mode' : 'Switch to dark mode');
    }
  }

  const savedTheme = localStorage.getItem(STORAGE_KEY);
  if (savedTheme === 'dark' || savedTheme === 'light') {
    setTheme(savedTheme);
  } else {
    setTheme(mediaQuery.matches ? 'dark' : 'light');
  }

  if (toggle) {
    toggle.addEventListener('click', () => {
      const current = root.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
      setTheme(current === 'dark' ? 'light' : 'dark');
    });
  }
})();
