/* ========================================
   Vantage Advisory Group
   Interactive Behaviors
   ======================================== */

(function () {
    'use strict';

    // --- Scrolled Nav ---
    const nav = document.getElementById('nav');
    function updateNav() {
        nav.classList.toggle('scrolled', window.scrollY > 60);
    }
    window.addEventListener('scroll', updateNav, { passive: true });
    updateNav();

    // --- Mobile Nav Toggle ---
    const toggle = document.getElementById('nav-toggle');
    const links = document.getElementById('nav-links');

    toggle.addEventListener('click', function () {
        links.classList.toggle('open');
        const isOpen = links.classList.contains('open');
        toggle.setAttribute('aria-expanded', isOpen);
    });

    // Close mobile nav on link click
    links.querySelectorAll('a').forEach(function (link) {
        link.addEventListener('click', function () {
            links.classList.remove('open');
            toggle.setAttribute('aria-expanded', 'false');
        });
    });

    // --- Scroll Fade-In ---
    const faders = document.querySelectorAll('.failure-card, .process-step, .principle, .engagement-card, .callout');

    faders.forEach(function (el) {
        el.classList.add('fade-in');
    });

    const observer = new IntersectionObserver(function (entries) {
        entries.forEach(function (entry) {
            if (entry.isIntersecting) {
                entry.target.classList.add('visible');
                observer.unobserve(entry.target);
            }
        });
    }, {
        threshold: 0.15,
        rootMargin: '0px 0px -40px 0px'
    });

    faders.forEach(function (el) {
        observer.observe(el);
    });

    // --- Contact Form ---
    var form = document.getElementById('contact-form');
    form.addEventListener('submit', function (e) {
        e.preventDefault();
        var btn = form.querySelector('button[type="submit"]');
        var originalText = btn.textContent;
        btn.textContent = 'Message Sent';
        btn.disabled = true;
        btn.style.opacity = '0.7';
        setTimeout(function () {
            btn.textContent = originalText;
            btn.disabled = false;
            btn.style.opacity = '1';
            form.reset();
        }, 3000);
    });
})();
