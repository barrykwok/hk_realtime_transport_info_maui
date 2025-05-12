using System;
using Microsoft.Maui;
using Microsoft.Maui.Hosting;

namespace hk_realtime_transport_info_maui;

class Program : MauiApplication
{
	protected override MauiApp CreateMauiApp() => MauiProgram.CreateMauiApp();

	static void Main(string[] args)
	{
		var app = new Program();
		app.Run(args);
	}
}
