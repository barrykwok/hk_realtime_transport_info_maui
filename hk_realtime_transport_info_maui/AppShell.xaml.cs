using hk_realtime_transport_info_maui.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Maui.Controls.Xaml;
using System.Reflection;

namespace hk_realtime_transport_info_maui;

public partial class AppShell : Shell
{
	private readonly LiteDbService _databaseService;
	private readonly KmbDataService _kmbDataService;
	private readonly EtaService _etaService;
	private readonly ILogger<MainPage> _logger;
	private MainPage _mainPage;

	public AppShell(LiteDbService databaseService, KmbDataService kmbDataService, EtaService etaService, ILogger<MainPage> logger)
	{
		// Manually initialize components from XAML
		InitializeComponent();
		
		_databaseService = databaseService;
		_kmbDataService = kmbDataService;
		_etaService = etaService;
		_logger = logger;
		
		// Set title from resources
		Title = App.GetString("AppTitle", "HK Transport Info");
		
		// Create the main page instance and keep a reference
		_mainPage = new MainPage(_databaseService, _kmbDataService, _etaService, _logger);
		
		// Register routes
		Routing.RegisterRoute(nameof(MainPage), typeof(MainPage));
		Routing.RegisterRoute(nameof(RouteDetailsPage), typeof(RouteDetailsPage));
		
		// Set the main page
		CurrentItem = new ShellContent
		{
			Content = _mainPage,
			Route = nameof(MainPage),
			Title = "Home"
		};
	}
	
	// Language selection handlers
	private void OnEnglishClicked(object sender, EventArgs e)
	{
		App.ChangeCulture("en-US");
		UpdateUI();
	}
	
	private void OnTraditionalChineseClicked(object sender, EventArgs e)
	{
		App.ChangeCulture("zh-HK");
		UpdateUI();
	}
	
	private void OnSimplifiedChineseClicked(object sender, EventArgs e)
	{
		App.ChangeCulture("zh-CN");
		UpdateUI();
	}
	
	// Helper method to refresh the UI after changing culture
	private void UpdateUI()
	{
		// Update the shell title
		Title = App.GetString("AppTitle", "HK Transport Info");
		
		// Refresh the main page
		if (_mainPage != null)
		{
			// Notify the main page to update its UI
			_mainPage.OnLanguageChanged();
		}
		
		// Close the drawer menu
		FlyoutIsPresented = false;
	}
}
