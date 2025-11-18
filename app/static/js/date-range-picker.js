/**
 * ODCHC Date Range Picker Component
 * A vanilla JavaScript date picker with preset ranges and dual calendar view
 * No dependencies required
 */

class DateRangePicker {
    constructor(options = {}) {
        this.options = {
            container: options.container || document.body,
            onApply: options.onApply || null,
            minYear: options.minYear || 2020, // ODCHC established in 2020
            maxYear: options.maxYear || new Date().getFullYear(),
            maxRangeDays: options.maxRangeDays || 365,
            initialStartDate: options.initialStartDate || null,
            initialEndDate: options.initialEndDate || null,
        };

        this.state = {
            fromDate: this.options.initialStartDate ? new Date(this.options.initialStartDate) : null,
            toDate: this.options.initialEndDate ? new Date(this.options.initialEndDate) : null,
            activeTab: 'quick',
            selectedYear: new Date().getFullYear(),
            monthlyType: 'monthly',
            leftCalendarDate: new Date(),
            rightCalendarDate: new Date(new Date().getFullYear(), new Date().getMonth() + 1),
            lastAppliedPreset: null,
            isSelecting: false,
        };

        this.element = null;
        this.init();
    }

    init() {
        this.createDOM();
        this.attachEventListeners();
        this.updateCalendars();
        this.updateStatusDisplay();
    }

    createDOM() {
        const picker = document.createElement('div');
        picker.className = 'odchc-date-picker';
        picker.innerHTML = `
            <div class="odchc-date-picker__overlay"></div>
            <div class="odchc-date-picker__container">
                <div class="odchc-date-picker__header">
                    <h3>Date Range</h3>
                    <button type="button" class="odchc-date-picker__close" aria-label="Close">×</button>
                </div>
                <div class="odchc-date-picker__body">
                    <!-- Left Column: Presets -->
                    <div class="odchc-date-picker__left">
                        <!-- Tabs -->
                        <div class="odchc-date-picker__tabs">
                            <button type="button" class="odchc-date-picker__tab active" data-tab="quick">Quick</button>
                            <button type="button" class="odchc-date-picker__tab" data-tab="quarterly">Qtr</button>
                            <button type="button" class="odchc-date-picker__tab" data-tab="monthly">Mon</button>
                        </div>

                        <!-- Quick Tab Content -->
                        <div class="odchc-date-picker__tab-content active" data-content="quick">
                            <button type="button" class="odchc-date-picker__preset" data-preset="today">Today</button>
                            <button type="button" class="odchc-date-picker__preset" data-preset="yesterday">Yesterday</button>
                            <button type="button" class="odchc-date-picker__preset" data-preset="this_week">This Week</button>
                            <button type="button" class="odchc-date-picker__preset" data-preset="this_month">This Month</button>
                            <button type="button" class="odchc-date-picker__preset" data-preset="last_month">Last Month</button>
                            <button type="button" class="odchc-date-picker__preset" data-preset="last_90">Last 90 Days</button>
                            <button type="button" class="odchc-date-picker__preset" data-preset="ytd">Year to Date</button>
                        </div>

                        <!-- Quarterly Tab Content -->
                        <div class="odchc-date-picker__tab-content" data-content="quarterly">
                            <div class="odchc-date-picker__year-selector">
                                <label>Year:</label>
                                <select class="odchc-date-picker__year-select" data-year-type="quarterly">
                                    ${this.generateYearOptions()}
                                </select>
                            </div>
                            <div class="odchc-date-picker__quarter-grid">
                                <button type="button" class="odchc-date-picker__quarter" data-quarter="1">
                                    <span class="quarter-label">Q1</span>
                                    <span class="quarter-months">Jan - Mar</span>
                                </button>
                                <button type="button" class="odchc-date-picker__quarter" data-quarter="2">
                                    <span class="quarter-label">Q2</span>
                                    <span class="quarter-months">Apr - Jun</span>
                                </button>
                                <button type="button" class="odchc-date-picker__quarter" data-quarter="3">
                                    <span class="quarter-label">Q3</span>
                                    <span class="quarter-months">Jul - Sep</span>
                                </button>
                                <button type="button" class="odchc-date-picker__quarter" data-quarter="4">
                                    <span class="quarter-label">Q4</span>
                                    <span class="quarter-months">Oct - Dec</span>
                                </button>
                            </div>
                        </div>

                        <!-- Monthly Tab Content -->
                        <div class="odchc-date-picker__tab-content" data-content="monthly">
                            <div class="odchc-date-picker__year-selector">
                                <label>Year:</label>
                                <select class="odchc-date-picker__year-select" data-year-type="monthly">
                                    ${this.generateYearOptions()}
                                </select>
                            </div>
                            <div class="odchc-date-picker__period-type">
                                <label>
                                    <input type="radio" name="monthly-type" value="monthly" checked>
                                    Monthly
                                </label>
                                <label>
                                    <input type="radio" name="monthly-type" value="bimonthly">
                                    Bi-Monthly
                                </label>
                            </div>
                            <div class="odchc-date-picker__month-grid">
                                ${this.generateMonthButtons()}
                            </div>
                        </div>
                    </div>

                    <!-- Right Column: Calendars -->
                    <div class="odchc-date-picker__right">
                        <div class="odchc-date-picker__calendars">
                            <div class="odchc-date-picker__calendar" data-calendar="left"></div>
                            <div class="odchc-date-picker__calendar" data-calendar="right"></div>
                        </div>
                        <div class="odchc-date-picker__status">
                            <div class="odchc-date-picker__status-row">
                                <strong>From:</strong> <span data-status="from">-</span>
                            </div>
                            <div class="odchc-date-picker__status-row">
                                <strong>To:</strong> <span data-status="to">-</span>
                            </div>
                        </div>
                        <button type="button" class="odchc-date-picker__apply">Apply</button>
                    </div>
                </div>
            </div>
        `;

        this.element = picker;
        this.options.container.appendChild(picker);
    }

    generateYearOptions() {
        let options = '';
        for (let year = this.options.maxYear; year >= this.options.minYear; year--) {
            const selected = year === this.state.selectedYear ? 'selected' : '';
            options += `<option value="${year}" ${selected}>${year}</option>`;
        }
        return options;
    }

    generateMonthButtons() {
        const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        return months.map((month, idx) =>
            `<button type="button" class="odchc-date-picker__month" data-month="${idx}">${month}</button>`
        ).join('');
    }

    attachEventListeners() {
        const picker = this.element;

        // Close button
        picker.querySelector('.odchc-date-picker__close').addEventListener('click', () => this.close());
        picker.querySelector('.odchc-date-picker__overlay').addEventListener('click', () => this.close());

        // Tab switching
        picker.querySelectorAll('.odchc-date-picker__tab').forEach(tab => {
            tab.addEventListener('click', (e) => this.switchTab(e.target.dataset.tab));
        });

        // Preset buttons
        picker.querySelectorAll('.odchc-date-picker__preset').forEach(btn => {
            btn.addEventListener('click', (e) => this.applyPreset(e.target.dataset.preset));
        });

        // Quarter buttons
        picker.querySelectorAll('.odchc-date-picker__quarter').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const quarter = parseInt(e.currentTarget.dataset.quarter);
                this.applyQuarter(quarter);
            });
        });

        // Month buttons (will be regenerated dynamically)
        this.attachMonthListeners();

        // Year selectors
        picker.querySelectorAll('.odchc-date-picker__year-select').forEach(select => {
            select.addEventListener('change', (e) => {
                this.state.selectedYear = parseInt(e.target.value);
                // Sync all year selects
                picker.querySelectorAll('.odchc-date-picker__year-select').forEach(s => {
                    s.value = this.state.selectedYear;
                });
            });
        });

        // Monthly type radio buttons
        picker.querySelectorAll('input[name="monthly-type"]').forEach(radio => {
            radio.addEventListener('change', (e) => {
                this.state.monthlyType = e.target.value;
                this.updateMonthGrid();
            });
        });

        // Apply button
        picker.querySelector('.odchc-date-picker__apply').addEventListener('click', () => this.apply());

        // Keyboard support
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.isOpen()) {
                this.close();
            }
        });
    }

    attachMonthListeners() {
        this.element.querySelectorAll('.odchc-date-picker__month').forEach(btn => {
            btn.addEventListener('click', (e) => {
                const month = parseInt(e.target.dataset.month);
                this.applyMonth(month);
            });
        });
    }

    switchTab(tabName) {
        this.state.activeTab = tabName;

        // Update active tab button
        this.element.querySelectorAll('.odchc-date-picker__tab').forEach(tab => {
            tab.classList.toggle('active', tab.dataset.tab === tabName);
        });

        // Update active content
        this.element.querySelectorAll('.odchc-date-picker__tab-content').forEach(content => {
            content.classList.toggle('active', content.dataset.content === tabName);
        });
    }

    applyPreset(preset) {
        const today = new Date();
        today.setHours(0, 0, 0, 0);

        let fromDate, toDate;

        switch (preset) {
            case 'today':
                fromDate = new Date(today);
                toDate = new Date(today);
                break;
            case 'yesterday':
                fromDate = new Date(today);
                fromDate.setDate(fromDate.getDate() - 1);
                toDate = new Date(fromDate);
                break;
            case 'this_week':
                fromDate = new Date(today);
                fromDate.setDate(fromDate.getDate() - fromDate.getDay()); // Start of week (Sunday)
                toDate = new Date(today);
                break;
            case 'this_month':
                fromDate = new Date(today.getFullYear(), today.getMonth(), 1);
                toDate = new Date(today);
                break;
            case 'last_month':
                fromDate = new Date(today.getFullYear(), today.getMonth() - 1, 1);
                toDate = new Date(today.getFullYear(), today.getMonth(), 0); // Last day of previous month
                break;
            case 'last_90':
                fromDate = new Date(today);
                fromDate.setDate(fromDate.getDate() - 90);
                toDate = new Date(today);
                break;
            case 'ytd':
                fromDate = new Date(today.getFullYear(), 0, 1);
                toDate = new Date(today);
                break;
        }

        this.setDateRange(fromDate, toDate, preset);
    }

    applyQuarter(quarter) {
        const year = this.state.selectedYear;
        const startMonth = (quarter - 1) * 3;
        const endMonth = startMonth + 2;

        const fromDate = new Date(year, startMonth, 1);
        const toDate = new Date(year, endMonth + 1, 0); // Last day of end month

        this.setDateRange(fromDate, toDate, `q${quarter}-${year}`);
    }

    applyMonth(month) {
        const year = this.state.selectedYear;

        if (this.state.monthlyType === 'monthly') {
            const fromDate = new Date(year, month, 1);
            const toDate = new Date(year, month + 1, 0);
            this.setDateRange(fromDate, toDate, `month-${month + 1}-${year}`);
        } else {
            // Bi-monthly: pair months (0-1, 2-3, 4-5, etc.)
            const pairIndex = Math.floor(month / 2);
            const startMonth = pairIndex * 2;
            const endMonth = startMonth + 1;

            const fromDate = new Date(year, startMonth, 1);
            const toDate = new Date(year, endMonth + 1, 0);
            this.setDateRange(fromDate, toDate, `bimonth-${pairIndex}-${year}`);
        }
    }

    updateMonthGrid() {
        const monthGrid = this.element.querySelector('.odchc-date-picker__month-grid');

        if (this.state.monthlyType === 'monthly') {
            monthGrid.innerHTML = this.generateMonthButtons();
        } else {
            // Generate bi-monthly buttons
            const bimonths = [
                'Jan-Feb', 'Mar-Apr', 'May-Jun',
                'Jul-Aug', 'Sep-Oct', 'Nov-Dec'
            ];
            monthGrid.innerHTML = bimonths.map((label, idx) =>
                `<button type="button" class="odchc-date-picker__month odchc-date-picker__month--bimonthly" data-month="${idx * 2}">${label}</button>`
            ).join('');
        }

        this.attachMonthListeners();
    }

    setDateRange(fromDate, toDate, preset = null) {
        // Ensure fromDate <= toDate
        if (fromDate > toDate) {
            [fromDate, toDate] = [toDate, fromDate];
        }

        this.state.fromDate = fromDate;
        this.state.toDate = toDate;
        this.state.lastAppliedPreset = preset;

        // Navigate calendars to show the selected range
        this.state.leftCalendarDate = new Date(fromDate.getFullYear(), fromDate.getMonth(), 1);
        this.state.rightCalendarDate = new Date(toDate.getFullYear(), toDate.getMonth(), 1);

        // If both dates are in same month, show next month on right
        if (this.state.leftCalendarDate.getTime() === this.state.rightCalendarDate.getTime()) {
            this.state.rightCalendarDate.setMonth(this.state.rightCalendarDate.getMonth() + 1);
        }

        this.updateCalendars();
        this.updateStatusDisplay();
        this.updatePresetActiveStates();
    }

    renderCalendar(container, date, isLeft) {
        const year = date.getFullYear();
        const month = date.getMonth();
        const firstDay = new Date(year, month, 1);
        const lastDay = new Date(year, month + 1, 0);
        const daysInMonth = lastDay.getDate();
        const startingDayOfWeek = firstDay.getDay();

        const monthNames = ['January', 'February', 'March', 'April', 'May', 'June',
                          'July', 'August', 'September', 'October', 'November', 'December'];

        let html = `
            <div class="odchc-calendar__header">
                ${isLeft ? `<button type="button" class="odchc-calendar__nav odchc-calendar__nav--prev" data-dir="prev">◀</button>` : ''}
                <span class="odchc-calendar__title">${monthNames[month]} ${year}</span>
                ${!isLeft ? `<button type="button" class="odchc-calendar__nav odchc-calendar__nav--next" data-dir="next">▶</button>` : ''}
            </div>
            <div class="odchc-calendar__grid">
                <div class="odchc-calendar__day-header">Su</div>
                <div class="odchc-calendar__day-header">Mo</div>
                <div class="odchc-calendar__day-header">Tu</div>
                <div class="odchc-calendar__day-header">We</div>
                <div class="odchc-calendar__day-header">Th</div>
                <div class="odchc-calendar__day-header">Fr</div>
                <div class="odchc-calendar__day-header">Sa</div>
        `;

        // Empty cells before first day
        for (let i = 0; i < startingDayOfWeek; i++) {
            html += '<div class="odchc-calendar__day odchc-calendar__day--empty"></div>';
        }

        // Days of month
        const today = new Date();
        today.setHours(0, 0, 0, 0);

        for (let day = 1; day <= daysInMonth; day++) {
            const currentDate = new Date(year, month, day);
            const classes = ['odchc-calendar__day'];

            // Check if it's today
            if (currentDate.getTime() === today.getTime()) {
                classes.push('odchc-calendar__day--today');
            }

            // Check if selected
            if (this.state.fromDate && currentDate.getTime() === this.state.fromDate.getTime()) {
                classes.push('odchc-calendar__day--selected-start');
            }
            if (this.state.toDate && currentDate.getTime() === this.state.toDate.getTime()) {
                classes.push('odchc-calendar__day--selected-end');
            }

            // Check if in range
            if (this.state.fromDate && this.state.toDate &&
                currentDate > this.state.fromDate && currentDate < this.state.toDate) {
                classes.push('odchc-calendar__day--in-range');
            }

            // Disable future dates
            if (currentDate > today) {
                classes.push('odchc-calendar__day--disabled');
            }

            html += `<button type="button" class="${classes.join(' ')}" data-date="${year}-${month}-${day}" ${currentDate > today ? 'disabled' : ''}>${day}</button>`;
        }

        html += '</div>';
        container.innerHTML = html;

        // Attach event listeners
        container.querySelectorAll('.odchc-calendar__day:not(.odchc-calendar__day--disabled):not(.odchc-calendar__day--empty)').forEach(dayBtn => {
            dayBtn.addEventListener('click', (e) => {
                const [y, m, d] = e.target.dataset.date.split('-').map(Number);
                this.selectDate(new Date(y, m, d));
            });
        });

        // Navigation buttons
        const prevBtn = container.querySelector('.odchc-calendar__nav--prev');
        const nextBtn = container.querySelector('.odchc-calendar__nav--next');

        if (prevBtn) {
            prevBtn.addEventListener('click', () => this.navigateCalendar('prev'));
        }
        if (nextBtn) {
            nextBtn.addEventListener('click', () => this.navigateCalendar('next'));
        }
    }

    selectDate(date) {
        if (!this.state.fromDate || (this.state.fromDate && this.state.toDate)) {
            // Start new selection
            this.state.fromDate = date;
            this.state.toDate = null;
            this.state.lastAppliedPreset = null;
        } else {
            // Complete selection
            if (date < this.state.fromDate) {
                this.state.toDate = this.state.fromDate;
                this.state.fromDate = date;
            } else {
                this.state.toDate = date;
            }
        }

        this.updateCalendars();
        this.updateStatusDisplay();
        this.updatePresetActiveStates();
    }

    navigateCalendar(direction) {
        const offset = direction === 'prev' ? -1 : 1;

        this.state.leftCalendarDate.setMonth(this.state.leftCalendarDate.getMonth() + offset);
        this.state.rightCalendarDate.setMonth(this.state.rightCalendarDate.getMonth() + offset);

        this.updateCalendars();
    }

    updateCalendars() {
        const leftContainer = this.element.querySelector('[data-calendar="left"]');
        const rightContainer = this.element.querySelector('[data-calendar="right"]');

        this.renderCalendar(leftContainer, this.state.leftCalendarDate, true);
        this.renderCalendar(rightContainer, this.state.rightCalendarDate, false);
    }

    updateStatusDisplay() {
        const fromSpan = this.element.querySelector('[data-status="from"]');
        const toSpan = this.element.querySelector('[data-status="to"]');

        fromSpan.textContent = this.state.fromDate
            ? this.formatDate(this.state.fromDate)
            : '-';
        toSpan.textContent = this.state.toDate
            ? this.formatDate(this.state.toDate)
            : '-';
    }

    updatePresetActiveStates() {
        // Clear all active states
        this.element.querySelectorAll('.odchc-date-picker__preset, .odchc-date-picker__quarter, .odchc-date-picker__month').forEach(btn => {
            btn.classList.remove('active');
        });

        // Highlight active preset if applicable
        if (this.state.lastAppliedPreset) {
            const activeBtn = this.element.querySelector(`[data-preset="${this.state.lastAppliedPreset}"], [data-quarter="${this.state.lastAppliedPreset}"]`);
            if (activeBtn) {
                activeBtn.classList.add('active');
            }
        }
    }

    formatDate(date) {
        const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
        return `${months[date.getMonth()]} ${date.getDate()}, ${date.getFullYear()}`;
    }

    apply() {
        if (!this.state.fromDate || !this.state.toDate) {
            alert('Please select both start and end dates');
            return;
        }

        const result = {
            fromDate: this.state.fromDate,
            toDate: this.state.toDate,
            fromDateString: this.formatDate(this.state.fromDate),
            toDateString: this.formatDate(this.state.toDate),
            preset: this.state.lastAppliedPreset
        };

        if (this.options.onApply) {
            this.options.onApply(result);
        }

        this.close();
    }

    open() {
        this.element.classList.add('active');
        document.body.style.overflow = 'hidden';
    }

    close() {
        this.element.classList.remove('active');
        document.body.style.overflow = '';
    }

    isOpen() {
        return this.element.classList.contains('active');
    }

    destroy() {
        if (this.element) {
            this.element.remove();
        }
    }
}

// Export for use
window.DateRangePicker = DateRangePicker;
