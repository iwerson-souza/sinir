const DEFAULT_START_DATE = '2020-01-01';

const getUtcDateFromString = (usDateFormat: string) => {
    const [y, m, d] = usDateFormat.split('-').map(Number);
    return new Date(Date.UTC(y, m - 1, d, 0, 0, 0, 0));
};

// const getUtcDateFromDate = (date: Date, dateOnly: boolean) => {
//     return new Date(
//         Date.UTC(
//             date.getUTCFullYear(),
//             date.getUTCMonth(),
//             date.getUTCDate(),
//             dateOnly ? 0 : date.getUTCHours(),
//             dateOnly ? 0 : date.getUTCMinutes(),
//             dateOnly ? 0 : date.getUTCSeconds(),
//             dateOnly ? 0 : date.getUTCMilliseconds(),
//         ),
//     );
// };

const getPeriods = (startDate: Date, endDate: Date) => {
    const periods: { startDate: Date; endDate: Date }[] = [];

    let year = startDate.getUTCFullYear();
    let month = startDate.getUTCMonth();

    const endYear = endDate.getUTCFullYear();
    const endMonth = endDate.getUTCMonth();

    while (year < endYear || (year === endYear && month <= endMonth)) {
        const isStartMonth = year === startDate.getUTCFullYear() && month === startDate.getUTCMonth();
        const firstDay = new Date(Date.UTC(year, month, isStartMonth ? startDate.getUTCDate() : 1));
        const lastDay = new Date(Date.UTC(year, month + 1, 0));

        if (year === endYear && month === endMonth && lastDay > endDate) {
            lastDay.setUTCDate(endDate.getUTCDate());
        }

        periods.push({ startDate: firstDay, endDate: lastDay });

        if (month === 11) {
            month = 0;
            year++;
        } else {
            month++;
        }
    }

    return periods;
};

export const buildStrategy = (unidade: string, lastEndDate?: Date) => {
    const baseUrl = 'https://mtr.sinir.gov.br/api/mtr/pesquisaManifestoRelatorioMtrAnalitico';
    const urlTemplates = [
        '/{ID}/18/8/{START_DATE}/{END_DATE}/5/0/9/0',
        '/{ID}/18/5/{START_DATE}/{END_DATE}/8/0/9/0',
        '/{ID}/18/9/{START_DATE}/{END_DATE}/8/0/5/0',
    ];

    // start date is the day after the last end date or a default start date
    const calcStartDate = lastEndDate
        ? new Date(Date.UTC(lastEndDate.getUTCFullYear(), lastEndDate.getUTCMonth(), lastEndDate.getUTCDate() + 1))
        : getUtcDateFromString(DEFAULT_START_DATE);

    calcStartDate.setUTCHours(0, 0, 0, 0);

    // yesterday's date in UTC
    const now = new Date();
    const calcEndDate = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 1));
    calcEndDate.setUTCHours(0, 0, 0, 0);

    const periods = getPeriods(calcStartDate, calcEndDate);
    const format = (date: Date) =>
        `${String(date.getUTCDate()).padStart(2, '0')}-${String(date.getUTCMonth() + 1).padStart(2, '0')}-${date.getUTCFullYear()}`;

    const urls = [];
    for (const period of periods) {
        urls.push({
            ...period,
            urls: urlTemplates.map(template => {
                const url =
                    baseUrl +
                    template.replace('{ID}', unidade).replace('{START_DATE}', format(period.startDate)).replace('{END_DATE}', format(period.endDate));
                return url;
            }),
        });
    }

    return {
        summary: { startDate: calcStartDate, finalDate: calcEndDate },
        setup: urls,
    };
};
